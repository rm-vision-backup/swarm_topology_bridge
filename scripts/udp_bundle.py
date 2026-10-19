#!/usr/bin/env python3
"""udp_bundle.py — bundle 的 UDP 单播传输（纯 stdlib，无 ROS，可单测）。

背景：TCP/ZMQ bundle 的 ACK/重传/队头阻塞在 13 机时占到接近一半包数。本模块把
同一个 bundle 帧作为一条 UDP datagram 发给每个对端，去掉 ACK/重传；合包格式
（bundle.py）与 Service 路径不变。

约定：
- 一帧 bundle = 一条 UDP datagram；一个 tick 多帧时按 (源, seq) 重组。
- 源身份用 (src_ip, src_port)：实机各机 IP 不同、port_offset=0；仿真全部
  127.0.0.1，用 port_offset 区分。因此必须绑定本机 (uav_ip, bundle_port+offset)，
  失败时 fail-fast，不能悄悄退回 0.0.0.0（否则源身份不可判定）。
"""
from __future__ import annotations

import select
import socket

MAX_UDP_PAYLOAD = 65507
MTU_SAFE_BYTES = 1472
DEFAULT_SOCKET_BUFFER = 4 * 1024 * 1024


def _endpoint(uav_ip, port_offsets, name, bundle_port):
    return (uav_ip[name], int(bundle_port) + int(port_offsets.get(name, 0)))


def build_udp_peer_maps(my_name, uav_ip, port_offsets, topology, recv_sources, bundle_port):
    """返回 (local_addr, addr_to_src, dsts)。

    - local_addr：本机 UDP 绑定/发送地址，保证源 IP/端口确定。
    - addr_to_src：{(src_ip, src_port): src_name}，用于接收端 demux。
    - dsts：[(peer_name, (peer_ip, peer_port))]，按 peer 名排序；
      peer 集合 = {P | my_name in topology[P]}，排除自身。
    地址冲突（同 IP+端口映射到两个名字）属于配置错误，直接抛 ValueError。
    """
    if my_name not in uav_ip:
        raise ValueError("UDP: my_name %r 不在 uavs 列表中" % (my_name,))
    local_addr = _endpoint(uav_ip, port_offsets, my_name, bundle_port)

    addr_to_src = {}
    for src in recv_sources:
        if src not in uav_ip:
            raise ValueError("UDP: source %r 不在 uavs 列表中" % (src,))
        addr = _endpoint(uav_ip, port_offsets, src, bundle_port)
        if addr == local_addr:
            raise ValueError("UDP: 源地址 %r 与本机绑定地址相同（检查 IP/port_offset）" % (addr,))
        if addr in addr_to_src and addr_to_src[addr] != src:
            raise ValueError("UDP: 源地址 %r 同时映射到 %s 和 %s"
                             % (addr, addr_to_src[addr], src))
        addr_to_src[addr] = src

    dsts = []
    seen = {}
    for peer, sources in topology.items():
        if peer == my_name or my_name not in sources:
            continue
        if peer not in uav_ip:
            raise ValueError("UDP: 目的端 %r 不在 uavs 列表中" % (peer,))
        addr = _endpoint(uav_ip, port_offsets, peer, bundle_port)
        if addr == local_addr:
            raise ValueError("UDP: 目的地址 %r 与本机绑定地址相同（检查 IP/port_offset）" % (addr,))
        if addr in seen and seen[addr] != peer:
            raise ValueError("UDP: 目的地址 %r 同时映射到 %s 和 %s"
                             % (addr, seen[addr], peer))
        seen[addr] = peer
        dsts.append((peer, addr))
    dsts.sort(key=lambda item: item[0])
    return local_addr, addr_to_src, dsts


class UdpBundleSocket:
    """bundle 的 UDP 单播 socket。send_frames 由发送线程独占；recv_batch 由接收线程独占。

    同一个 fd 同时 sendto/recvfrom 在 OS 层是安全的；本类不跨线程共享状态。
    """

    def __init__(self, local_addr, rcvbuf=DEFAULT_SOCKET_BUFFER, sndbuf=DEFAULT_SOCKET_BUFFER):
        self.local_addr = (str(local_addr[0]), int(local_addr[1]))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, int(rcvbuf))
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, int(sndbuf))
            except OSError:
                pass                      # 内核可能限制缓冲上限；继续用默认值
            self.sock.bind(self.local_addr)
            self.sock.setblocking(False)
        except OSError:
            try:
                self.sock.close()
            finally:
                raise
        self.effective_rcvbuf = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        self.effective_sndbuf = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)

    @property
    def bound_addr(self):
        return self.sock.getsockname()

    def send_frames(self, frames, dsts):
        """每帧 x 每个目的端一条 datagram。返回计数 dict，绝不因单个目的地抛异常。"""
        counters = {"datagrams": 0, "errors": 0, "oversize": 0, "blocked": 0}
        for frame in frames:
            if len(frame) > MAX_UDP_PAYLOAD:
                counters["oversize"] += 1
                continue
            for _peer, addr in dsts:
                try:
                    self.sock.sendto(frame, addr)
                    counters["datagrams"] += 1
                except BlockingIOError:
                    counters["blocked"] += 1
                except OSError:
                    counters["errors"] += 1
        return counters

    def recv_batch(self, max_batch=64, timeout=0.1):
        """等待并批量收包，返回 [(data, (ip, port), truncated)]。"""
        try:
            ready, _, _ = select.select([self.sock], [], [], float(timeout))
        except (OSError, ValueError):
            return []
        if not ready:
            return []
        out = []
        for _ in range(max(1, int(max_batch))):
            try:
                if hasattr(self.sock, "recvmsg"):
                    data, _anc, flags, addr = self.sock.recvmsg(65536)
                    truncated = bool(flags & getattr(socket, "MSG_TRUNC", 0))
                else:                     # pragma: no cover - Linux 都有 recvmsg
                    data, addr = self.sock.recvfrom(65536)
                    truncated = False
            except BlockingIOError:
                break
            except OSError:
                break
            out.append((data, (addr[0], int(addr[1])), truncated))
        return out

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class BundleReassembler:
    """按 (源, seq) 重组一个 tick 的多帧；单线程使用（recv 线程）。"""

    def __init__(self):
        self._seq = None
        self._entries = []
        self._frames = 0
        self._bytes = 0
        self._last_completed_seq = None
        self.incomplete_ticks = 0

    def feed(self, seq, more, entries, nbytes=0):
        """喂入一帧，返回 (completed_entries_or_None, discarded_incomplete_ticks)。

        v1 线格式没有 frame index/count：若中间帧丢失、末帧到达，只能提交部分 entries；
        已知限制。新 seq 到来时未完成的 tick 会整体丢弃并计数。
        """
        discarded = 0
        if seq == self._last_completed_seq:
            return None, discarded          # 重复 tick（UDP 乱序/重复）
        if self._seq is None:
            self._seq = seq
        elif seq != self._seq:
            if self._entries:
                discarded = 1
                self.incomplete_ticks += 1
            self._seq = seq
            self._entries = []
            self._frames = 0
            self._bytes = 0

        self._entries.extend(entries)
        self._frames += 1
        self._bytes += int(nbytes)

        if more:
            return None, discarded

        completed = self._entries
        self._entries = []
        self._frames = 0
        self._bytes = 0
        self._last_completed_seq = seq
        self._seq = None
        return completed, discarded
