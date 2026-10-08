#!/usr/bin/env python3
# 基于拓扑配置的 ROS ZeroMQ 桥接节点
# 功能:
#   1) Topic 桥 (ZMQ PUB/SUB), 保留 ROS 消息原生序列化与来源命名空间
#   2) 通用 Service 请求/响应代理 (ZMQ DEALER/ROUTER):
#      - 配置声明 Service 名称、类型、目标节点和超时
#      - 本机为每个远程 Service 注册同名 ROS Service, 上层 ROS 调用透明转发
#      - 每次请求携带 bridge 层唯一 request ID
#      - 支持并发请求、超时和迟到响应丢弃
#      - bridge 不理解任务业务, 只代理 ROS 序列化请求/响应
#
# 限频:
#   - 每个话题独立限频配置; max_freq 为 null/0 表示不限频
#   - 启动窗口首条消息永不丢弃
#
# 数据面观测（2026-10-08 增，见 bridge_health.py 模块头）:
#   - 每话题收发计数/到达间隔统计，按秒写 JSONL 到 health_log_dir（不经 rosout，直接写盘）；
#   - 停顿看门狗（只检测+记录，不重建 socket）；
#   - ZMQ 心跳 / TCP keepalive / 重连退避，让链路半开能被判定并自动重连。
import os
import sys
import traceback

import rospy
import roslib.message
import zmq
import threading
import time
import socket
import io
from threading import Lock

def _import_bridge_health():
    """定位同目录的 bridge_health.py。

    直接跑源码脚本时 ``__file__`` 就在 scripts/ 下；catkin devel/install 空间的 relay 也会执行源文件，
    但 ``__file__`` 可能是 relay 自身路径——所以再自底向上找几层，找不到**大声报错**（静默降级等于
    下一次实飞还是黑箱，这正是要修的问题）。
    """
    here = os.path.dirname(os.path.abspath(__file__))
    for candidate in (here, os.path.dirname(here),
                      os.path.join(here, "..", "..", "..", "scripts")):
        candidate = os.path.abspath(candidate)
        if os.path.isfile(os.path.join(candidate, "bridge_health.py")) and candidate not in sys.path:
            sys.path.insert(0, candidate)
            break
    else:
        sys.path.insert(0, here)
    from bridge_health import (LinkStats, HealthWriter, apply_link_options,  # noqa: E402
                               ConnectionMonitor, read_loadavg, read_self_cpu_seconds)
    return LinkStats, HealthWriter, apply_link_options, ConnectionMonitor, read_loadavg, read_self_cpu_seconds


(LinkStats, HealthWriter, apply_link_options,
 ConnectionMonitor, read_loadavg, read_self_cpu_seconds) = _import_bridge_health()

DEFAULT_HEALTH_KEY_TOPICS = (
    "mavros/local_position/odom",
    "mavros/local_position/pose",
    "mavros/state",
    "trajectory_intent",
    "uav_task_state",
)
# 看门狗默认只盯"高频周期性"话题：trajectory_intent / uav_task_state / gp_origin_confirmed 是
# 事件驱动或一次性（首帧之后长期静默属正常），把它们纳入会持续产生假停顿事件。
DEFAULT_STALL_TOPICS = (
    "mavros/local_position/odom",
    "mavros/local_position/pose",
)
# 连接级事件默认只监控 odom（关键链路）；对每个被监控话题每源一个 monitor socket。
DEFAULT_MONITOR_TOPICS = ("mavros/local_position/odom",)


def ensure_global(name):
    return name if name.startswith('/') else '/' + name


class BridgeNode:
    def __init__(self):
        # 1) 读取参数与本机标识
        self.ns = rospy.get_namespace()
        self.uavs = rospy.get_param('~uavs')
        self.topics = rospy.get_param('~topics')
        self.max_freq = float(rospy.get_param('~max_freq', 0))
        self.base_port = int(rospy.get_param('~base_port'))
        self.topology = rospy.get_param('~topology')
        self.services = rospy.get_param('~services', [])
        self.service_base_port = int(rospy.get_param('~service_base_port', self.base_port + 10000))

        override_name = rospy.get_param('~uav_name', None)
        self.my_name = override_name if override_name else socket.gethostname()

        name_set = set([u['name'] for u in self.uavs])
        if self.my_name not in name_set:
            my_ip = socket.gethostbyname(socket.gethostname())
            candidates = [u['name'] for u in self.uavs if u['ip'] == my_ip]
            if len(candidates) == 1:
                self.my_name = candidates[0]
            else:
                rospy.logerr("bridge: 无法根据 hostname/IP 解析本机 UAV 名称, 请在 launch 中设置参数 ~uav_name")
                raise RuntimeError("uav_name not resolved")

        # 2) 基础映射与上下文
        self.uav_ip = {u['name']: u.get('ip', '127.0.0.1') for u in self.uavs}
        self.topic_ports = {i: self.base_port + i for i in range(len(self.topics))}
        self.uav_port_offsets = {u['name']: int(u.get('port_offset', 0)) for u in self.uavs}

        self.context = zmq.Context.instance()
        self.pub_sockets = {}
        self.pub_locks = {}
        self.sub_sockets = []
        self.poller = zmq.Poller()
        self.socket_map = {}
        self.msg_classes = {}
        self.publishers = {}
        self.window_start = {}
        self.send_counts = {}
        # 每话题限频; None 表示不限频
        self.topic_limits = {}
        self._load_health_config()

        # 3) 动态加载消息类型
        for i, t in enumerate(self.topics):
            cls = roslib.message.get_message_class(t['type'])
            if cls is None:
                rospy.logerr("bridge: invalid msg type %s", t['type'])
                raise RuntimeError("invalid msg type %s" % t['type'])
            self.msg_classes[i] = cls
            limit = t.get('max_freq', self.max_freq)
            self.topic_limits[i] = None if limit is None or float(limit) <= 0 else float(limit)

        for i in range(len(self.topics)):
            self.window_start[i] = time.time()
            self.send_counts[i] = 0

        # 4) 初始化 ZMQ PUB
        my_offset = self.uav_port_offsets.get(self.my_name, 0)
        for i, _ in enumerate(self.topics):
            s = self.context.socket(zmq.PUB)
            self._link_options_applied = self._apply_link_options(s)
            s.bind("tcp://*:%d" % (self.topic_ports[i] + my_offset))
            self.pub_sockets[i] = s
            self.pub_locks[i] = Lock()
        rospy.loginfo("bridge: my_name=%s, port_offset=%d, topics=%d, health=%s",
                      self.my_name, my_offset, len(self.topics),
                      self.health.path or "(disabled)")

        # 5) 初始化 ZMQ SUB 与对应 ROS Publisher
        src_list = self.topology.get(self.my_name, [])
        rospy.loginfo("bridge: recv sources=%s", ",".join(src_list) if src_list else "(none)")
        for src in src_list:
            ip = self.uav_ip[src]
            src_offset = self.uav_port_offsets.get(src, 0)
            for i in range(len(self.topics)):
                s = self.context.socket(zmq.SUB)
                self._apply_link_options(s)
                s.setsockopt(zmq.SUBSCRIBE, b"")
                if i in self.monitor_indices:
                    # 连接级证据：只有它能把"链路断/重连"与"链路在但没数据"分开。
                    # 必须在 connect() 之前挂上，否则会漏掉首次 connected（环回链路尤其快）。
                    self._monitors.append(ConnectionMonitor(
                        s,
                        lambda name, endpoint, value, _src=src, _idx=i:
                        self._on_link_event(_src, _idx, name, endpoint, value),
                        label="%s/%s" % (src, self.topic_names.get(i, i))))
                s.connect("tcp://%s:%d" % (ip, self.topic_ports[i] + src_offset))
                self.sub_sockets.append(s)
                self.poller.register(s, zmq.POLLIN)
                self.socket_map[s] = (src, i)
                pub_name = ensure_global(self.topics[i]['name'])
                pub_full = "/%s%s" % (src, pub_name)
                self.publishers[(src, i)] = rospy.Publisher(pub_full, self.msg_classes[i], queue_size=10)

        # 6) 初始化 ROS 订阅
        for i, t in enumerate(self.topics):
            sub_name = ensure_global(t['name'])
            cls = self.msg_classes[i]
            rospy.Subscriber(sub_name, cls, self._make_cb(i), queue_size=10)

        # 7) 启动 Topic 接收线程
        self.recv_thread = threading.Thread(target=self.recv_loop)
        self.recv_thread.daemon = True
        self.recv_thread.start()

        # 8) 初始化通用 Service 代理
        self._service_init()

        # 9) 健康写盘 / 停顿看门狗线程（数据面观测，只在有 path 时实际落盘）
        self.health_thread = threading.Thread(target=self._health_loop)
        self.health_thread.daemon = True
        self.health_thread.start()
        rospy.loginfo("bridge: link options %s", ",".join(self._link_options_applied) or "(none)")
        self._record_bridge_event({
            "event": "health_start", "path": self.health.path or "", "host": socket.gethostname(),
            "sample_hz": self.health_sample_hz, "stall_warn_s": self.stall_warn_s,
            "matrix_period_s": self.health_matrix_period_s,
            "key_topics": sorted(self.health_key_topics),
            "stall_topics": sorted(self.stall_topics),
            "monitor_topics": sorted(self.monitor_topics),
            "link_options": self._link_options_applied,
        })
        rospy.on_shutdown(self._shutdown_health)

    # --------------------------------------------------------------- 观测配置
    def _load_health_config(self):
        """读取观测/链路参数；**任何异常都降级为"观测关闭"，绝不阻断 bridge 启动**。

        实飞前最后的加固：观测是"附加价值"，桥接本体是"必须活着"——参数写错（如
        `_health_sample_hz:=abc`）时宁可没有健康日志，也不能让 bridge 起不来导致整机失联。
        """
        try:
            self._load_health_config_unsafe()
        except Exception as exc:                     # noqa: BLE001
            rospy.logerr("bridge: 观测/链路参数非法（%s: %s）——已关闭观测，桥接继续",
                         type(exc).__name__, exc)
            self._install_disabled_health()

    def _install_disabled_health(self):
        """观测关闭时的最小状态（链路加固仍用默认值生效）。"""
        self.health = HealthWriter(None)
        self.stats = LinkStats()
        self.health_sample_hz = 1.0
        self.health_matrix_period_s = 0.0
        self.stall_warn_s = 0.0                      # 0 = 不判停顿
        self.health_key_topics = set()
        self.stall_topics = set()
        self.monitor_topics = set()
        self.link_options = {}
        self.dealer_sndtimeo_ms = 500
        self.topic_names = {i: t["name"] for i, t in enumerate(self.topics)}
        self.key_indices = set()
        self.stall_indices = set()
        self.monitor_indices = set()
        self._monitors = []
        self._recv_errors = {}
        self._link_options_applied = []
        self._link_lock = threading.Lock()
        self._link_seen = set()
        self._link_connected = set()
        self._link_suppressed = {}

    def _load_health_config_unsafe(self):
        """实际的参数读取（可能因非法参数抛异常，由 `_load_health_config` 兜底）。"""
        default_dir = os.path.expanduser("~/catkin_swarm6-2/.runtime_logs/health")
        health_dir = str(rospy.get_param("~health_log_dir", default_dir) or "").strip()
        if health_dir.lower() in ("none", "off", "disabled"):
            health_dir = ""
        self.health = HealthWriter(
            os.path.join(health_dir, "%s_bridge_health.jsonl" % self.my_name) if health_dir else None,
            warn=lambda message: rospy.logerr_throttle(30.0, "bridge: %s", message))
        self.stats = LinkStats()
        self.health_sample_hz = max(0.2, float(rospy.get_param("~health_sample_hz", 0.5)))
        # 逐 (源,话题) 明细体积是汇总行的 10 倍以上，默认每 10 s 一行（汇总行仍是每秒一行）。
        self.health_matrix_period_s = max(0.0, float(rospy.get_param("~health_matrix_period_s", 60.0)))
        self.stall_warn_s = float(rospy.get_param("~stall_warn_s", 1.5))
        key_topics = rospy.get_param("~health_key_topics", list(DEFAULT_HEALTH_KEY_TOPICS))
        stall_topics = rospy.get_param("~stall_topics", list(DEFAULT_STALL_TOPICS))
        monitor_topics = rospy.get_param("~monitor_topics", list(DEFAULT_MONITOR_TOPICS))
        self.health_key_topics = set(key_topics or [])
        self.stall_topics = set(stall_topics or [])
        self.monitor_topics = set(monitor_topics or [])
        self.link_options = dict(
            heartbeat_ivl_ms=int(rospy.get_param("~zmq_heartbeat_ivl_ms", 1000)),
            # 现场实测电台拥塞时 1~3 s 空档（config/topology.yaml:30-34）：超时取 10 s（≥2× 实测最差），
            # 否则 3 s 超时会把"半开兜底"变成高频拆链重建。
            heartbeat_timeout_ms=int(rospy.get_param("~zmq_heartbeat_timeout_ms", 10000)),
            tcp_keepalive=1 if rospy.get_param("~zmq_tcp_keepalive", True) else 0,
            reconnect_ivl_ms=int(rospy.get_param("~zmq_reconnect_ivl_ms", 200)),
            # 退避上限 1 s（不取 3 s）：跟随者 1.0 s 就判 LEADER_LOST，恢复太慢等于没恢复。
            reconnect_ivl_max_ms=int(rospy.get_param("~zmq_reconnect_ivl_max_ms", 1000)),
        )
        # DEALER 满 SNDHWM 时会阻塞 send，给有界等待（见 bridge_health.apply_link_options 注释）。
        self.dealer_sndtimeo_ms = int(rospy.get_param("~zmq_dealer_sndtimeo_ms", 500))
        self.topic_names = {i: t["name"] for i, t in enumerate(self.topics)}
        self.key_indices = {i for i, name in self.topic_names.items()
                            if name in self.health_key_topics}
        self.stall_indices = {i for i, name in self.topic_names.items()
                              if name in self.stall_topics}
        self.monitor_indices = {i for i, name in self.topic_names.items()
                                if name in self.monitor_topics}
        self._monitors = []
        self._recv_errors = {}          # socket -> 连续异常次数（自旋保护）
        self._link_options_applied = []
        self._link_lock = threading.Lock()
        self._link_seen = set()         # 收到过 monitor 事件的 (src, idx)
        self._link_connected = set()    # 至少连上过一次的 (src, idx)
        self._link_suppressed = {}      # 未连接端点的重试计数（不逐条刷日志）

    def _apply_link_options(self, sock, **extra):
        """给 socket 装链路选项；失败只降级（返回已生效的选项名），绝不抛出。"""
        try:
            return apply_link_options(sock, **self.link_options, **extra)
        except Exception as exc:                     # noqa: BLE001
            rospy.logwarn_throttle(30.0, "bridge: link options failed (%s)", exc)
            return []

    def _on_link_event(self, src, idx, name, endpoint, value):
        """连接级事件。对"从未连上的端点"只计数不刷日志——生产 topology 列了 17 个节点、
        实际只飞几架，未起飞的对端会以 ~1 Hz 反复 connect_retried/closed，实测占事件量
        99%（SITL 彩排 16 事件/s 全来自此）；连上一次之后的断开/重连才是要看的。"""
        with self._link_lock:
            self._link_seen.add((src, idx))
            connected = (src, idx) in self._link_connected
        if name == "connected":
            with self._link_lock:
                self._link_connected.add((src, idx))
        elif (src, idx) not in self._link_connected:
            with self._link_lock:
                self._link_suppressed[(src, idx)] = self._link_suppressed.get((src, idx), 0) + 1
            return
        self._record_bridge_event({"event": "link", "link_event": name, "src": src,
                                   "topic": self.topic_names.get(idx, str(idx)),
                                   "endpoint": endpoint, "value": value})

    def _shutdown_health(self):
        for monitor in self._monitors:
            monitor.close()
        self.health.close()

    def _record_bridge_event(self, record):
        record.setdefault("uav", self.my_name)
        self.health.emit(record)

    def _record_error(self, sock, stage, exc):
        """接收线程内的异常：写事件 + 节流 rosout，**不再让线程退出**。"""
        src, idx = self.socket_map.get(sock, ("?", -1))
        record = {"event": "error", "stage": stage, "src": src,
                  "topic": self.topic_names.get(idx, str(idx)),
                  "error": "%s: %s" % (type(exc).__name__, exc),
                  "traceback": traceback.format_exc()[-2000:]}
        self._record_bridge_event(record)
        rospy.logerr_throttle(5.0, "bridge: %s failed on %s/%s: %s",
                              stage, src, self.topic_names.get(idx, idx), exc)

    def _stuck_sockets(self):
        """本轮"套接字里有数据但没被取走"的 (src, topic) 列表。

        收包线程被饿死/卡住时，ZMQ 会在 socket 上置 POLLIN 而 rx 不前进——这是把
        "线程没在收"与"链路没数据"分开的直接证据。
        """
        stuck = []
        candidates = self.monitor_indices or self.key_indices
        for sock, (src, idx) in list(self.socket_map.items()):
            if idx not in candidates:
                continue
            try:
                events = int(sock.getsockopt(zmq.EVENTS))
            except Exception:                              # noqa: BLE001
                continue
            if events & zmq.POLLIN:
                stuck.append("%s/%s" % (src, self.topic_names.get(idx, idx)))
        return stuck

    def _health_loop(self):
        period = 1.0 / self.health_sample_hz
        last_matrix = 0.0
        # 周期性 matrix 已降频到 60 s；若有人手动调密（≤15 s）则不需要停顿时再补一份
        matrix_on_small_period = 0.0 < self.health_matrix_period_s <= 15.0
        while not rospy.is_shutdown():
            try:
                now = time.monotonic()
                stalled = False
                for event in self.stats.poll_stalls(
                        now, self.stall_warn_s,
                        key_filter=lambda scope, src, idx: idx in self.stall_indices):
                    event["topic"] = self.topic_names.get(event.pop("topic_idx"), "")
                    self._record_bridge_event(event)
                    stalled = True
                if stalled and not matrix_on_small_period:
                    # 停顿发生时立刻补一份逐 (源,话题) 明细：这正是需要"看清单"的时刻。
                    self.health.emit({"event": "matrix", "uav": self.my_name,
                                      "trigger": "stall",
                                      "remote": self.stats.matrix(self.topic_names,
                                                                  self.key_indices, now=now)})
                    last_matrix = now
                if self.health.path is not None:
                    sample = self.stats.summary(self.topic_names, self.key_indices, now=now,
                                                stale_age_s=self.stall_warn_s)
                    with self._link_lock:
                        seen = len(self._link_seen)
                        unconnected = len(self._link_seen - self._link_connected)
                        retries = sum(self._link_suppressed.values())
                    sample.update({"event": "sample", "uav": self.my_name,
                                   "load1": read_loadavg(),
                                   "cpu_s": read_self_cpu_seconds(),
                                   "write_errors": self.health.errors,
                                   "stuck_sockets": self._stuck_sockets(),
                                   "links_seen": seen, "links_unconnected": unconnected,
                                   "link_retries_suppressed": retries})
                    self.health.emit(sample)
                    if self.health_matrix_period_s > 0.0 and \
                            now - last_matrix >= self.health_matrix_period_s:
                        last_matrix = now
                        self.health.emit({"event": "matrix", "uav": self.my_name,
                                          "remote": self.stats.matrix(self.topic_names,
                                                                      self.key_indices, now=now)})
            except Exception as exc:                       # noqa: BLE001 - 观测线程绝不退出
                rospy.logerr_throttle(30.0, "bridge: health loop error (%s)", exc)
            time.sleep(period)

    # ------------------------------------------------------------------ Topic
    def _make_cb(self, idx):
        def cb(msg):
            now = time.time()
            self.stats.note_local_rx(idx)
            elapsed = now - self.window_start[idx]
            limit = self.topic_limits[idx]
            discard = False
            if limit is not None:
                # 首条消息永不丢弃; 滑窗内超过限频才丢弃
                if self.send_counts[idx] > 0 and elapsed > 0 and \
                        (self.send_counts[idx] + 1) / elapsed > limit:
                    discard = True
            if discard:
                # 记丢弃数：否则"local_rx=30 Hz / tx=20 Hz"与"代码把消息丢了"无法区分。
                self.stats.note_local_drop(idx)
            if not discard:
                buff = io.BytesIO()
                msg.serialize(buff)
                payload = buff.getvalue()
                with self.pub_locks[idx]:
                    self.pub_sockets[idx].send(payload, flags=0)
                self.send_counts[idx] += 1
                self.stats.note_tx(idx)
            # 滑窗重置: 周期为 1s
            if elapsed > 1.0:
                self.window_start[idx] = now
                self.send_counts[idx] = 0
        return cb

    def recv_loop(self):
        while not rospy.is_shutdown():
            events = dict(self.poller.poll(timeout=100))
            for s in events:
                if events[s] != zmq.POLLIN:
                    continue
                # 无 try/except 的旧实现里，任何一次反序列化/发布异常都会静默杀死本线程，
                # 该机**所有**远端话题永久停转，且日志零记录（2026-09-22/09-23 排查时无法区分
                # "线程死了"与"链路没数据"）。现改为：记事件、跳过该条、继续服务。
                if s in self._recv_errors and self._recv_errors[s] >= 5:
                    time.sleep(0.05)                   # 自旋保护：持续异常时不要把 poll 打成忙循环
                try:
                    payload = s.recv(flags=0)
                except Exception as exc:               # noqa: BLE001
                    self._recv_errors[s] = self._recv_errors.get(s, 0) + 1
                    if not rospy.is_shutdown():
                        self._record_error(s, "recv", exc)
                    continue
                src, idx = self.socket_map[s]
                self._recv_errors.pop(s, None)
                try:
                    msg = self.msg_classes[idx]()
                    msg.deserialize(payload)
                except Exception as exc:               # noqa: BLE001
                    self._record_error(s, "deserialize", exc)
                    continue
                self.stats.note_rx(src, idx)
                try:
                    self.publishers[(src, idx)].publish(msg)
                except Exception as exc:               # noqa: BLE001
                    self._record_error(s, "publish", exc)

    # --------------------------------------------------------------- Service
    def _service_init(self):
        """初始化通用 Service 请求/响应代理。

        规则 (每个节点加载同一份配置):
          - target == my_name: 本机是服务端, 绑定 ROUTER 端口, 收到跨机请求后
            调用本地同名 ROS Service 并返回序列化响应。
          - target != my_name: 本机是潜在请求端, 在本机注册同名 ROS Service,
            上层 ROS 调用时通过 ZMQ 转发到目标节点。
        """
        self.srv_classes = {}
        self.local_srv_proxies = {}   # name -> rospy.ServiceProxy (服务端用)
        self.dealers = {}             # target -> zmq.DEALER
        self.dealer_locks = {}
        self._request_counter = 0
        self._req_lock = Lock()
        self.pending_requests = {}    # request_id -> _PendingRequest
        self.dealer_recv_threads = []

        # 收集 service 定义
        for item in self.services:
            name = ensure_global(item['name'])
            srv_type = item['type']
            cls = roslib.message.get_service_class(srv_type)
            if cls is None:
                rospy.logerr("bridge: invalid service type %s", srv_type)
                raise RuntimeError("invalid service type %s" % srv_type)
            self.srv_classes[name] = cls

        # 服务端: 目标为本节点时绑定 ROUTER
        self.router = None
        if any(ensure_global(item['name']) in self.srv_classes and item.get('target') == self.my_name
               for item in self.services):
            my_offset = self.uav_port_offsets.get(self.my_name, 0)
            self.router = self.context.socket(zmq.ROUTER)
            # ROUTER 的响应发送同样会有界等待：它服务**本机全部**服务的请求，
            # 若因某个请求端管道满而阻塞，会连带停掉本机所有服务路径。
            self._apply_link_options(self.router, sndtimeo_ms=self.dealer_sndtimeo_ms)
            self.router.bind("tcp://*:%d" % (self.service_base_port + my_offset))
            self.router_thread = threading.Thread(target=self._router_loop)
            self.router_thread.daemon = True
            self.router_thread.start()
            rospy.loginfo("bridge: service router bound on %d (my_name=%s)",
                          self.service_base_port + my_offset, self.my_name)

        # 请求端: 为每个远程 service 注册本地 ROS Service 代理
        for item in self.services:
            name = ensure_global(item['name'])
            target = item['target']
            if target == self.my_name:
                # 服务端: 缓存本地 ServiceProxy
                self.local_srv_proxies[name] = rospy.ServiceProxy(name, self.srv_classes[name], persistent=False)
            else:
                # 请求端: 注册本地 ROS Service, 转发到目标节点
                rospy.Service(name, self.srv_classes[name], self._make_service_handler(name, target))
                rospy.loginfo("bridge: proxying %s -> %s", name, target)
                self._ensure_dealer(target, item)

    def _ensure_dealer(self, target, item):
        if target in self.dealers:
            return
        ip = self.uav_ip[target]
        target_offset = self.uav_port_offsets.get(target, 0)
        dealer = self.context.socket(zmq.DEALER)
        # DEALER 满 SNDHWM 时 send_multipart 会阻塞（PUB 不会）→ 有界等待，避免服务路径无声挂起。
        self._apply_link_options(dealer, sndtimeo_ms=self.dealer_sndtimeo_ms)
        dealer.connect("tcp://%s:%d" % (ip, self.service_base_port + target_offset))
        self.dealers[target] = dealer
        self.dealer_locks[target] = Lock()
        # 后台收线线程
        thread = threading.Thread(target=self._dealer_recv_loop, args=(target,))
        thread.daemon = True
        thread.start()
        self.dealer_recv_threads.append(thread)
        rospy.loginfo("bridge: dealer connected to %s service router %s:%d",
                      target, ip, self.service_base_port + target_offset)

    def _make_service_handler(self, name, target):
        def handler(req):
            request_id = self._new_request_id()
            buff = io.BytesIO()
            req.serialize(buff)
            payload = buff.getvalue()
            pending = _PendingRequest()
            with self._req_lock:
                self.pending_requests[request_id] = pending
            frames = [name.encode('utf-8'),
                      str(request_id).encode('ascii'),
                      payload]
            dealer = self.dealers[target]
            with self.dealer_locks[target]:
                try:
                    dealer.send_multipart(frames)
                except zmq.ZMQError as exc:
                    with self._req_lock:
                        self.pending_requests.pop(request_id, None)
                    self._record_bridge_event({"event": "error", "stage": "service_send",
                                               "target": target, "service": name,
                                               "error": "%s: %s" % (type(exc).__name__, exc)})
                    raise rospy.ServiceException("bridge send failed: %s" % exc)
            if not pending.event.wait(timeout=pending.timeout):
                with self._req_lock:
                    self.pending_requests.pop(request_id, None)
                raise rospy.ServiceException(
                    "bridge service timeout for %s -> %s" % (name, target))
            # 迟到响应已在 recv 线程中被丢弃
            if pending.error is not None:
                raise rospy.ServiceException(pending.error)
            return pending.response
        return handler

    def _dealer_recv_loop(self, target):
        # DEALER 接收端: ROUTER 发送的 identity 帧会被 ZMQ 自动剥离,
        # 因此收到的是 3 帧: [service_name, request_id, payload]。
        dealer = self.dealers[target]
        while not rospy.is_shutdown():
            try:
                frames = dealer.recv_multipart(flags=0)
            except zmq.ZMQError:
                if rospy.is_shutdown():
                    return
                continue
            if len(frames) != 3:
                rospy.logwarn("bridge: dropping malformed service reply from %s (%d frames)", target, len(frames))
                continue
            name_b, req_id_b, resp_payload = frames
            request_id = int(req_id_b.decode('ascii'))
            with self._req_lock:
                pending = self.pending_requests.pop(request_id, None)
            if pending is None:
                # 迟到/未知响应 -> 丢弃
                continue
            if resp_payload and resp_payload[0] == 1:
                pending.error = resp_payload[1:].decode('utf-8', errors='replace')
                pending.event.set()
                continue
            name = name_b.decode('utf-8')
            srv_cls = self.srv_classes.get(name)
            if srv_cls is None:
                pending.error = "bridge: unknown service %s" % name
                pending.event.set()
                continue
            if resp_payload[:1] == b'\x00':
                resp_payload = resp_payload[1:]
            response = srv_cls._response_class()
            try:
                # genpy.Message.deserialize requires bytes (indexable), not BytesIO.
                response.deserialize(resp_payload)
            except Exception as exc:
                pending.error = "bridge: bad response payload: %s" % exc
                pending.event.set()
                continue
            pending.response = response
            pending.event.set()

    def _router_loop(self):
        while not rospy.is_shutdown():
            try:
                frames = self.router.recv_multipart(flags=0)
            except zmq.ZMQError:
                if rospy.is_shutdown():
                    return
                continue
            if len(frames) != 4:
                rospy.logwarn("bridge: dropping malformed service request (%d frames)", len(frames))
                continue
            identity, name_b, req_id_b, req_payload = frames
            name = name_b.decode('utf-8')
            srv_cls = self.srv_classes.get(name)
            proxy = self.local_srv_proxies.get(name)
            if srv_cls is None or proxy is None:
                self.router.send_multipart([identity, name_b, req_id_b,
                                            b'\x01' + ("bridge: no local service %s" % name).encode('utf-8')])
                continue
            request = srv_cls._request_class()
            try:
                # genpy.Message.deserialize requires bytes (indexable), not BytesIO.
                request.deserialize(req_payload)
            except Exception as exc:
                self.router.send_multipart([identity, name_b, req_id_b,
                                            b'\x01' + ("bridge: bad request payload: %s" % exc).encode('utf-8')])
                continue
            try:
                response = proxy(request)
            except rospy.ServiceException as exc:
                self.router.send_multipart([identity, name_b, req_id_b,
                                            b'\x01' + str(exc).encode('utf-8')])
                continue
            buff = io.BytesIO()
            response.serialize(buff)
            self.router.send_multipart([identity, name_b, req_id_b, b'\x00' + buff.getvalue()])

    def _new_request_id(self):
        with self._req_lock:
            self._request_counter += 1
            return self._request_counter


class _PendingRequest:
    def __init__(self, timeout=2.0):
        self.event = threading.Event()
        self.response = None
        self.error = None
        self.timeout = timeout


def main():
    rospy.init_node('swarm_bridge', anonymous=False)
    BridgeNode()
    rospy.spin()


if __name__ == '__main__':
    main()