#!/usr/bin/env python3
"""bundle.py — 跨话题合包的纯逻辑（无 ROS，只 stdlib+threading；可无 ROS 单测，仿 bridge_health.py）。

背景：机间 mesh 是「每话题 × 每对端一条流、每帧一条消息」→ 大量小包把小包率的空口电台打满
（见 quality_reports/2026-10-12_airlink_scaling_test.md §8）。本模块把「一个源节点的全部话题、
每 tick 一帧」打包，把包数降约一个数量级。

线格式（每帧一条 ZMQ message；一 tick 多帧共享同一 seq，用 send_multipart 原子发）：
    HEADER    = <BBBIH>   magic(0x5A), version, flags, seq(u32), count(u16)      # 9 B
    ENTRY_HDR = <BI>      topic_idx(u8), msg_len(u32)                            # 5 B
    frame     := HEADER { ENTRY_HDR + payload[msg_len] } * count
flags.bit0(FLAG_MORE)=1 表示本 tick 还有后续帧。topic_idx = 发送端 topics 列表下标。

不读时钟：due 判定由调用方传入的单调 tick_index 决定（整数 tick 计数，无浮点漂移）。
"""
import struct
import threading
from collections import deque

MAGIC = 0x5A
VERSION = 1
FLAG_MORE = 0x01
DEFAULT_MAX_BYTES = 1400
DEFAULT_QUEUE_MAX = 100

_HEADER = struct.Struct("<BBBIH")      # magic, version, flags, seq, count
_ENTRY = struct.Struct("<BI")          # topic_idx, msg_len


class BundleError(Exception):
    """解码失败，reason ∈ {bad_magic, bad_version, truncated}。"""

    def __init__(self, reason, detail=""):
        super().__init__("%s%s" % (reason, (": " + str(detail)) if detail else ""))
        self.reason = reason


def encode_frame(seq, entries, more=False):
    """entries: list[(topic_idx:int, payload:bytes)] -> one frame (bytes)。"""
    flags = FLAG_MORE if more else 0
    parts = [_HEADER.pack(MAGIC, VERSION, flags, int(seq) & 0xFFFFFFFF, len(entries))]
    for idx, payload in entries:
        parts.append(_ENTRY.pack(idx, len(payload)))
        parts.append(payload)
    return b"".join(parts)


def decode_frame(frame):
    """one frame (bytes) -> (seq, more:bool, entries[: list[(idx,bytes)]])；非法抛 BundleError。"""
    if len(frame) < _HEADER.size:
        raise BundleError("truncated", "header")
    magic, version, flags, seq, count = _HEADER.unpack_from(frame, 0)
    if magic != MAGIC:
        raise BundleError("bad_magic", hex(magic))
    if version != VERSION:
        raise BundleError("bad_version", version)
    off = _HEADER.size
    out = []
    for _ in range(count):
        if off + _ENTRY.size > len(frame):
            raise BundleError("truncated", "entry header")
        idx, mlen = _ENTRY.unpack_from(frame, off)
        off += _ENTRY.size
        if off + mlen > len(frame):
            raise BundleError("truncated", "payload")
        out.append((idx, frame[off:off + mlen]))
        off += mlen
    return seq, bool(flags & FLAG_MORE), out


def decode_frames(frames):
    """list[bytes] -> list[(topic_idx, payload)]（按帧序拼接全部 entry）。"""
    entries = []
    for f in frames:
        entries.extend(decode_frame(f)[2])
    return entries


class TopicBuffer:
    """单话题在 tick 之间的缓冲。线程安全（内持 Lock）；不读时钟。

    mode:
      - latest：只保留最新值（周期话题；中间值可丢，覆盖计 drop）
      - queue ：全量保留、有界队列（事件话题；溢出丢最旧，计 drop）
    due：rate = min(max_freq, tick_hz)（max_freq<=0 → 不限），mod=max(1, round(tick_hz/rate))，
         tick_index % mod == 0 时 take_due 出栈；未到 due 时缓冲**不清空**。
    """

    def __init__(self, idx, mode, max_freq, tick_hz, queue_max=DEFAULT_QUEUE_MAX):
        self.idx = idx
        self.mode = mode if mode in ("latest", "queue") else "queue"
        self.queue_max = max(1, int(queue_max))
        self._lock = threading.Lock()
        self._one = None
        self._q = deque()
        self._dropped = 0
        mf = float(max_freq) if max_freq else 0.0
        if mf <= 0:
            self.mod = 1
        else:
            rate = min(mf, float(tick_hz))
            self.mod = max(1, int(round(float(tick_hz) / rate)))

    def add(self, payload):
        with self._lock:
            if self.mode == "latest":
                if self._one is not None:
                    self._dropped += 1
                self._one = payload
            else:
                self._q.append(payload)
                while len(self._q) > self.queue_max:
                    self._q.popleft()
                    self._dropped += 1

    def take_due(self, tick_index):
        with self._lock:
            if tick_index % self.mod != 0:
                return []
            if self.mode == "latest":
                if self._one is None:
                    return []
                out = [self._one]
                self._one = None
                return out
            out = list(self._q)
            self._q.clear()
            return out

    def dropped(self):
        with self._lock:
            return self._dropped

    def pending(self):
        with self._lock:
            if self.mode == "latest":
                return 1 if self._one is not None else 0
            return len(self._q)


class BundleBuilder:
    """把一批 (topic_idx, payload) 按 max_bytes 切成若干帧（entry 边界切）。"""

    def __init__(self, max_bytes=DEFAULT_MAX_BYTES):
        self.max_bytes = max(64, int(max_bytes))

    def build(self, seq, entries):
        if not entries:
            return []
        frames = []
        cur = []
        size = _HEADER.size

        def flush(more):
            frames.append(encode_frame(seq, cur, more=more))

        for entry in entries:
            esize = _ENTRY.size + len(entry[1])
            # 单条自身超限时允许独占一帧（cur 为空则必然不切），避免无限切分。
            if cur and size + esize > self.max_bytes:
                flush(True)
                cur = []
                size = _HEADER.size
            cur.append(entry)
            size += esize
        flush(False)
        return frames
