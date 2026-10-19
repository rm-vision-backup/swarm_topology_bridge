#!/usr/bin/env python3
"""桥接数据面观测与链路加固（纯逻辑：只依赖 stdlib + zmq，可无 ROS 单测）。

为什么存在：2026-09-22（三机）与 2026-09-23（四机）两轮实飞 coverage 都因**跟随者收不到领机
位姿流**（当时为 mavros/local_position/odom，2026-10-11 起改为轻量 swarm_pose）失败，而 bridge
对数据面**零可观测**——没有 per-topic 计数、没有最后到达时间、没有断链
日志，逐层定位只能靠排除法（见 quality_reports/2026-10-08_two_rounds_coverage_failure_root_cause.md）。

本模块提供四件事：

1. ``LinkStats``  —— 收发计数（含限频丢弃数）、到达间隔统计（max gap + 阈值计数）、停顿事件；
2. ``HealthWriter`` —— JSONL 追加写（每行 flush；IO 异常只节流告警，**绝不让观测拖垮 bridge**）；
3. ``apply_link_options`` —— ZMQ 心跳 / TCP keepalive / 重连退避 / DEALER 发送超时；
4. ``ConnectionMonitor`` —— 用 ZMQ socket monitor 拿**连接级**证据（连上/断开/重连），
   这是"链路在不在"的唯一直接信号（rx 计数只能证明"数据到没到"）。

口径与现场判据对齐：跟随环 ``_FOLLOW_POSE_TIMEOUT_S``（2026-10-10 由 1.0 s 放宽为 3.0 s，
ego_swarm.py；档位保留以便对照历史），因此间隔计数
固定统计 >0.2 / >0.5 / >1.0 / >3.0 s 四档，看门狗默认 1.5 s 告警。

配套事实（本次核实）：
- rospy 发布端队列满时**丢最旧、不阻塞**（``rospy/impl/tcpros_pubsub.py:414-417``），
  因此"接收线程被 publish 堵死"不成立——不必为此加固；
- ZMQ PUB 满 HWM 时**丢弃**、不阻塞；DEALER 满 SNDHWM 时**会阻塞**（故见 ``sndtimeo_ms``）。
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time

GAP_THRESHOLDS_S = (0.2, 0.5, 1.0, 3.0)


def _bump_gap(target: dict, gap_s: float) -> None:
    """把一次到达间隔记进 ``target``（dict 含 gap_max/gaps）。"""
    if gap_s < 0.0:
        return
    if gap_s > target["gap_max"]:
        target["gap_max"] = gap_s
    for thr in GAP_THRESHOLDS_S:
        if gap_s > thr:
            target["gaps"][thr] += 1


def _empty_row():
    return {"gap_max": 0.0, "gaps": {thr: 0 for thr in GAP_THRESHOLDS_S},
            "stall_since": None, "stalls": 0, "last_rx": None}


def _empty_bundle_row():
    """整源（bundle）行：在 _empty_row 基础上加帧/条/字节计数。"""
    return dict(_empty_row(), frames=0, msgs=0, bytes=0, incomplete=0)


def _gt_field(row):
    return {("gt%d" % int(thr * 1e3)): row["gaps"][thr] for thr in GAP_THRESHOLDS_S}


def read_loadavg():
    """本机 1 分钟平均负载（读不到返回 None）。用于分辨"线程饿死"与"链路没数据"。"""
    try:
        with open("/proc/loadavg") as handle:
            return float(handle.read().split()[0])
    except Exception:                                 # noqa: BLE001
        return None


def read_self_cpu_seconds():
    """本进程累计 CPU 秒（utime+stime）。相邻采样差可看 bridge 自身是否被饿死。"""
    try:
        with open("/proc/self/stat") as handle:
            fields = handle.read().rsplit(")", 1)[1].split()
        return (float(fields[11]) + float(fields[12])) / os.sysconf("SC_CLK_TCK")
    except Exception:                                 # noqa: BLE001
        return None


class LinkStats:
    """按 (源, 话题序号) / 话题序号 统计收发与到达间隔。线程安全。

    - 发送侧（本机 ROS 话题 → ZMQ）：``note_local_rx``（ROS 回调到达）、``note_tx``（限频后实发）、
      ``note_local_drop``（被限频丢弃——否则 local_rx 30 Hz / tx 20 Hz 与"代码把消息丢了"无法区分）；
    - 接收侧（ZMQ → 本机 ROS 话题）：``note_rx``。
    一把锁保护，``snapshot``/``summary`` 在锁内只做轻量拷贝，序列化/写盘由调用方在锁外完成。
    """

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._lock = threading.Lock()
        self._local: dict = {}     # topic_idx -> 行
        self._remote: dict = {}    # (src, topic_idx) -> 行
        self._reported: dict = {}  # 上次上报的计数（用于 drx）
        # 合包（packing=bundle）专用：整源级收发 + 本机发送侧累计
        self._bundles: dict = {}   # src -> 行（整源，未按话题拆）
        self._bundle_tx = {"frames": 0, "msgs": 0, "bytes": 0}
        # UDP bundle（transport=udp）专用计数；TCP 下保持全 0
        self._udp_tx = {"datagrams": 0, "errors": 0, "oversize": 0, "blocked": 0}
        self._udp_rx = {"unknown": 0, "decode": 0, "truncated": 0}

    # ---------------------------------------------------------------- 埋点
    def note_local_rx(self, idx):
        now = self._clock()
        with self._lock:
            row = self._local.setdefault(idx, dict(_empty_row(), rx=0, tx=0, drop=0))
            if row["last_rx"] is not None:
                _bump_gap(row, now - row["last_rx"])
            row["last_rx"] = now
            row["rx"] += 1

    def note_tx(self, idx):
        with self._lock:
            self._local.setdefault(idx, dict(_empty_row(), rx=0, tx=0, drop=0))["tx"] += 1

    def note_local_drop(self, idx, n=1):
        with self._lock:
            self._local.setdefault(idx, dict(_empty_row(), rx=0, tx=0, drop=0))["drop"] += int(n)

    def note_rx(self, src, idx):
        now = self._clock()
        with self._lock:
            row = self._remote.setdefault((src, idx), _empty_row())
            if row["last_rx"] is not None:
                _bump_gap(row, now - row["last_rx"])
            row["last_rx"] = now
            row["rx"] = row.get("rx", 0) + 1

    # --- 合包（packing=bundle）埋点：本机发送侧累计 + 逐源整包计数 -------
    def note_bundle_tx(self, frames, msgs, nbytes):
        """本机一个 tick 实发：帧数 / 消息条数 / 字节数。"""
        with self._lock:
            self._bundle_tx["frames"] += int(frames)
            self._bundle_tx["msgs"] += int(msgs)
            self._bundle_tx["bytes"] += int(nbytes)

    def note_bundle_rx(self, src, frames, msgs, nbytes=0):
        """收到一个源的 bundle：按整源计帧/条/字节并维护到达间隔（整源静默=最可靠停顿信号）。"""
        now = self._clock()
        with self._lock:
            row = self._bundles.setdefault(src, _empty_bundle_row())
            if row["last_rx"] is not None:
                _bump_gap(row, now - row["last_rx"])
            row["last_rx"] = now
            row["frames"] += int(frames)
            row["msgs"] += int(msgs)
            row["bytes"] += int(nbytes)

    # --- UDP bundle 埋点（transport=udp） -------------------------------
    def note_udp_tx(self, datagrams=0, errors=0, oversize=0, blocked=0):
        with self._lock:
            self._udp_tx["datagrams"] += int(datagrams)
            self._udp_tx["errors"] += int(errors)
            self._udp_tx["oversize"] += int(oversize)
            self._udp_tx["blocked"] += int(blocked)

    def note_udp_incomplete(self, src, n=1):
        """一个多帧 tick 因丢帧/乱序被丢弃。"""
        with self._lock:
            row = self._bundles.setdefault(src, _empty_bundle_row())
            row["incomplete"] = row.get("incomplete", 0) + int(n)

    def note_udp_rx_anomaly(self, kind, n=1):
        """kind ∈ {unknown, decode, truncated}。"""
        with self._lock:
            if kind in self._udp_rx:
                self._udp_rx[kind] += int(n)

    def udp_summary(self):
        with self._lock:
            return {
                "tx_datagrams": self._udp_tx["datagrams"],
                "tx_errors": self._udp_tx["errors"],
                "tx_oversize": self._udp_tx["oversize"],
                "tx_blocked": self._udp_tx["blocked"],
                "rx_unknown": self._udp_rx["unknown"],
                "rx_decode_errors": self._udp_rx["decode"],
                "rx_truncated": self._udp_rx["truncated"],
            }

    # ------------------------------------------------------------ 停顿检测
    def poll_stalls(self, now, warn_s, key_filter=None):
        """返回本轮的停顿事件（stall_start / stall_end），并维护内部停顿状态。

        ``key_filter``：``callable(scope, src, topic_idx) -> bool``，只对"周期性话题"告警
        （``trajectory_intent`` / ``uav_task_state`` / ``gp_origin_confirmed`` 是事件驱动或一次性，
        首帧之后长期静默属正常）。
        """
        events = []
        now = float(now)
        warn_s = float(warn_s)
        if warn_s <= 0.0:
            return events
        with self._lock:
            for (src, idx), row in self._remote.items():
                if key_filter is not None and not key_filter("remote", src, idx):
                    continue
                events.extend(self._pair_events(row, "remote", src, idx, now, warn_s))
            for idx, row in self._local.items():
                if key_filter is not None and not key_filter("local", None, idx):
                    continue
                events.extend(self._pair_events(row, "local", None, idx, now, warn_s))
            # 合包：整源静默是比"逐话题静默"更强的机间链路判据（scope="bundle"，src 必填）
            for src, row in self._bundles.items():
                if key_filter is not None and not key_filter("bundle", src, None):
                    continue
                events.extend(self._pair_events(row, "bundle", src, None, now, warn_s))
        return events

    @staticmethod
    def _pair_events(row, scope, src, idx, now, warn_s):
        events = []
        last = row["last_rx"]
        if last is None:
            return events                     # 从未有过流量：不算停顿（可能在等第一条）
        silent = now - last
        if silent > warn_s and row["stall_since"] is None:
            row["stall_since"] = last
            row["stalls"] += 1
            events.append({"event": "stall_start", "scope": scope, "src": src,
                           "topic_idx": idx, "stall_no": row["stalls"],
                           "silent_s": round(silent, 3)})
        elif silent <= warn_s and row["stall_since"] is not None:
            started = row["stall_since"]
            row["stall_since"] = None
            events.append({"event": "stall_end", "scope": scope, "src": src,
                           "topic_idx": idx,
                           "duration_s": round(now - started, 3)})
        return events

    # ------------------------------------------------------- 每秒汇总 / 明细
    def summary(self, names, key_indices, now=None, stale_age_s=1.5):
        """每秒一行：逐 KEY 话题的收发合计 + 最旧源（可直接看出"哪个源断了"）。

        对外按话题聚合而不是展开 (源 × 话题) 矩阵——矩阵体积是它的 10 倍以上，且会吃满采集端
        32 MB 上限（见 ``matrix()``，默认每 10 s 才发一次）。
        """
        now = self._clock() if now is None else float(now)
        local_rows, remote_rows = [], []
        with self._lock:
            for idx in sorted(key_indices):
                row = self._local.get(idx)
                if row is not None:
                    key = ("local", idx)
                    last_count = self._reported.get(key)
                    self._reported[key] = row["rx"]
                    local_rows.append({
                        "topic": names.get(idx, str(idx)), "rx": row["rx"], "tx": row["tx"],
                        "drop": row.get("drop", 0),
                        "drx": None if last_count is None else row["rx"] - last_count,
                        "age_ms": None if row["last_rx"] is None
                        else round((now - row["last_rx"]) * 1e3, 1),
                        "gap_max_ms": round(row["gap_max"] * 1e3, 1), "gt": _gt_field(row),
                        "stalls": row["stalls"],
                    })
            for idx in sorted(key_indices):
                srcs = [(src, row) for (src, i), row in self._remote.items() if i == idx]
                if not srcs:
                    continue
                total_rx = sum(row.get("rx", 0) for _, row in srcs)
                # 逐源汇总：最旧源 + 陈旧源个数（>stale_age_s）
                ages = [(src, None if row["last_rx"] is None else now - row["last_rx"])
                        for src, row in srcs]
                fresh = [(src, age) for src, age in ages if age is not None]
                age_max_src, age_max = (None, None)
                if fresh:
                    age_max_src, age_max = max(fresh, key=lambda item: item[1])
                stale = [{"src": src, "age_ms": round(age * 1e3, 1)}
                         for src, age in fresh if age > float(stale_age_s)]
                worst = max((row["gap_max"] for _, row in srcs), default=0.0)
                gt = {("gt%d" % int(thr * 1e3)):
                      sum(row["gaps"][thr] for _, row in srcs) for thr in GAP_THRESHOLDS_S}
                remote_rows.append({
                    "topic": names.get(idx, str(idx)), "rx": total_rx, "srcs": len(srcs),
                    "age_max_ms": None if age_max is None else round(age_max * 1e3, 1),
                    "age_max_src": age_max_src,
                    "stale_srcs": stale,
                    "gap_max_ms": round(worst * 1e3, 1), "gt": gt,
                    "stalls": sum(row["stalls"] for _, row in srcs),
                })
        sources = []
        with self._lock:
            for src, row in sorted(self._bundles.items()):
                sources.append({
                    "src": src, "frames": row["frames"], "msgs": row["msgs"],
                    "bytes": row["bytes"],
                    "age_ms": None if row["last_rx"] is None
                    else round((now - row["last_rx"]) * 1e3, 1),
                    "gap_max_ms": round(row["gap_max"] * 1e3, 1), "gt": _gt_field(row),
                    "stalls": row["stalls"],
                    "incomplete": row.get("incomplete", 0),
                })
        return {"local": local_rows, "remote": remote_rows, "sources": sources}

    def matrix(self, names, key_indices, now=None):
        """逐 (源, 话题) 明细（默认每 10 s 一行）：定位到具体哪条链。"""
        now = self._clock() if now is None else float(now)
        rows = []
        with self._lock:
            for (src, idx), row in sorted(self._remote.items(), key=lambda kv: (kv[0][1], kv[0][0])):
                if idx not in key_indices:
                    continue
                rows.append({
                    "src": src, "topic": names.get(idx, str(idx)), "rx": row.get("rx", 0),
                    "age_ms": None if row["last_rx"] is None
                    else round((now - row["last_rx"]) * 1e3, 1),
                    "gap_max_ms": round(row["gap_max"] * 1e3, 1), "gt": _gt_field(row),
                    "stalls": row["stalls"],
                })
        return rows

    def totals(self):
        """整体计数（用于日志/排障）：(local_rx, tx, drop, rx)。"""
        with self._lock:
            return (sum(r["rx"] for r in self._local.values()),
                    sum(r["tx"] for r in self._local.values()),
                    sum(r.get("drop", 0) for r in self._local.values()),
                    sum(r.get("rx", 0) for r in self._remote.values()))

    def bundle_totals(self):
        """合包整体计数：(本机发帧数, 发条数, 发字节数, 收帧数)。"""
        with self._lock:
            return (self._bundle_tx["frames"], self._bundle_tx["msgs"],
                    self._bundle_tx["bytes"],
                    sum(r["frames"] for r in self._bundles.values()))

class HealthWriter:
    """JSONL 追加写。``path=None`` → 空实现。

    契约：目录按需创建；每行 flush；任何异常只节流告警（``warn`` 回调，默认写 stderr）、绝不抛出。
    写失败计数经 ``errors`` 暴露（调用方把它写进汇总行——"没写盘"本身必须可见）。
    """

    def __init__(self, path=None, label="bridge_health", warn=None):
        self.path = str(path) if path else None
        self.label = label
        self._warn = warn
        self._lock = threading.Lock()
        self._handle = None
        self._disabled = False
        self._last_warn = 0.0
        self._errors = 0

    @property
    def errors(self):
        with self._lock:
            return self._errors

    def emit(self, record: dict) -> None:
        if self.path is None or self._disabled:
            return
        record = dict(record)
        record.setdefault("t", round(time.time(), 6))
        try:
            with self._lock:
                if self._handle is None:
                    os.makedirs(os.path.dirname(self.path), exist_ok=True)
                    self._handle = open(self.path, "a")
                self._handle.write(json.dumps(record, ensure_ascii=False,
                                              default=str) + "\n")
                self._handle.flush()
        except Exception as exc:                      # noqa: BLE001 - 观测绝不抛出
            with self._lock:
                self._errors += 1
            now = time.monotonic()
            if now - self._last_warn > 30.0:
                self._last_warn = now
                message = "%s: health log write failed (%s); further errors suppressed" % (
                    self.label, exc)
                if self._warn is not None:
                    try:
                        self._warn(message)
                    except Exception:                 # noqa: BLE001
                        sys.stderr.write(message + "\n")
                else:
                    sys.stderr.write(message + "\n")
            if isinstance(exc, (OSError, ValueError)):
                self._disabled = True                 # 目录不可写/已关闭：不再重试刷屏

    def close(self) -> None:
        with self._lock:
            if self._handle is not None:
                try:
                    self._handle.close()
                except OSError:
                    pass
                self._handle = None


def apply_link_options(sock, heartbeat_ivl_ms=1000, heartbeat_timeout_ms=10000,
                       tcp_keepalive=1, tcp_keepalive_idle_s=30, tcp_keepalive_intvl_s=10,
                       tcp_keepalive_cnt=3, reconnect_ivl_ms=200, reconnect_ivl_max_ms=1000,
                       sndtimeo_ms=0):
    """给一个 ZMQ socket 装链路健康选项，返回实际生效的选项名列表。

    必须在 ``bind()`` / ``connect()`` **之前**调用（libzmq 在会话建立时快照选项）。

    - ``HEARTBEAT_IVL`` / ``HEARTBEAT_TIMEOUT``：ZMTP 心跳。超时默认 10 s——现场实测电台拥塞时
      会出现 1~3 s 空档（config/topology.yaml:30-34），3 s 超时会把"半开兜底"变成高频拆链重建；
      心跳只覆盖"链路层"，**不覆盖对端进程活着但线程卡住**（那种情况由计数/停顿事件暴露）。
    - ``TCP_KEEPALIVE*``：OS 层半开检测（心跳之外的第二道）。
    - ``RECONNECT_IVL`` / ``RECONNECT_IVL_MAX``：断链重连退避——默认固定 100 ms 重试，在 ~160 条链
      同时掉时会以 10 Hz/链 的 SYN 抖电台；退避上限默认 1 s（不取 3 s：跟随者 1.0 s 就判 LEADER_LOST，
      恢复太慢等于没恢复）。
    - ``sndtimeo_ms``：DEALER 满 SNDHWM 时 ``send_multipart`` **会阻塞**（PUB 不会），把它变成
      有界等待 + 计数，避免服务路径无声挂起（本轮"组 abort 的 HOLD 没让领机停下"疑似与此有关）。
    """
    import zmq

    applied = []
    wanted = [
        ("HEARTBEAT_IVL", int(heartbeat_ivl_ms)),
        ("HEARTBEAT_TIMEOUT", int(heartbeat_timeout_ms)),
        ("TCP_KEEPALIVE", int(tcp_keepalive)),
        ("TCP_KEEPALIVE_IDLE", int(tcp_keepalive_idle_s)),
        ("TCP_KEEPALIVE_INTVL", int(tcp_keepalive_intvl_s)),
        ("TCP_KEEPALIVE_CNT", int(tcp_keepalive_cnt)),
        ("RECONNECT_IVL", int(reconnect_ivl_ms)),
        ("RECONNECT_IVL_MAX", int(reconnect_ivl_max_ms)),
    ]
    if int(sndtimeo_ms) > 0:
        wanted.append(("SNDTIMEO", int(sndtimeo_ms)))
    for name, value in wanted:
        opt = getattr(zmq, name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(opt, value)
            applied.append(name)
        except Exception:                             # noqa: BLE001 - 选项缺失/不支持时降级
            continue
    return applied


_MONITOR_EVENT_NAMES = (
    "EVENT_CONNECTED", "EVENT_CONNECT_DELAYED", "EVENT_CONNECT_RETRIED",
    "EVENT_LISTENING", "EVENT_ACCEPTED", "EVENT_CLOSED",
    "EVENT_DISCONNECTED", "EVENT_MONITOR_STOPPED",
    "EVENT_HANDSHAKE_SUCCEEDED", "EVENT_HANDSHAKE_FAILED_NO_DETAIL",
    "EVENT_HANDSHAKE_FAILED_PROTOCOL", "EVENT_HANDSHAKE_FAILED_AUTH",
)


def _monitor_event_name(value):
    """把 monitor 事件码翻成名字。pyzmq 18.1.1 只有 EVENT_CONNECT_RETRIED（无 EVENT_RETRIED）。"""
    import zmq

    for name in _MONITOR_EVENT_NAMES:
        code = getattr(zmq, name, None)
        if code is not None and int(code) == int(value):
            return name[6:].lower()          # 去掉 "EVENT_" 前缀
    return "code_%s" % value


class ConnectionMonitor:
    """订阅一个 socket 的 monitor 事件，把"连接级"事实回调出去（连上/断开/重连）。

    这是唯一能证明"链路在不在"的信号——rx 计数只能说明"数据到没到"。
    """

    def __init__(self, sock, on_event, label=""):
        self.label = label
        self._on_event = on_event
        self._monitor = None
        self._thread = None
        self._last_error_warn = 0.0
        try:
            self._monitor = sock.get_monitor_socket()
        except Exception:                             # noqa: BLE001 - monitor 不可用则静默降级
            self._monitor = None
            return
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True
        self._thread.start()

    def _loop(self):
        # 注意：`import zmq` 不会带出 zmq.utils.monitor 子模块——必须显式 import，
        # 否则每轮抛 AttributeError 被下面的 except 吞掉，表现为"monitor 一个事件都没有"。
        from zmq.utils.monitor import recv_monitor_message

        while self._monitor is not None:
            try:
                if not self._monitor.poll(500):
                    continue
                event = recv_monitor_message(self._monitor)
            except Exception as exc:                  # noqa: BLE001 - 观测线程绝不退出/抛出
                monitor = self._monitor
                if monitor is None or getattr(monitor, "closed", True):
                    return
                # 不静默：monitor 坏了要看得见（否则就是"零观测"本身复发）。
                now = time.monotonic()
                if now - self._last_error_warn > 30.0:
                    self._last_error_warn = now
                    sys.stderr.write("ConnectionMonitor %s: %s: %s\n"
                                     % (self.label, type(exc).__name__, exc))
                time.sleep(0.2)
                continue
            try:
                endpoint = event.get("endpoint", "")
                if isinstance(endpoint, bytes):       # pyzmq 给的是 bytes；不转成 str 会
                    endpoint = endpoint.decode("utf-8", "replace")   # 让 json.dumps 抛错被吞掉
                self._on_event(_monitor_event_name(event.get("event")),
                               endpoint, event.get("value", 0))
            except Exception:                         # noqa: BLE001
                continue

    def close(self):
        """真关 monitor socket 并收线程。

        只把引用置 None 是不够的：monitor socket 属于同一个 zmq.Context，不关掉会让
        `Context.term()`（节点退出、测试 teardown）一直等下去。
        """
        monitor, self._monitor = self._monitor, None
        if monitor is not None:
            try:
                monitor.close(linger=0)
            except Exception:                         # noqa: BLE001
                pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(1.0)
