#!/usr/bin/env python3
"""bridge_health 纯单元测试（无 ROS Master、无 DDS、无真实网络）。

覆盖：收发计数与限频丢弃、到达间隔（max gap + 阈值计数）、停顿起止事件、
每秒汇总（最旧源/陈旧源）、逐源明细、JSONL 写入与降级、ZMQ 链路选项回读、monitor 事件名映射。
"""
import json
import os
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import bridge_health  # noqa: E402


class FakeClock:
    def __init__(self, t=0.0):
        self.t = float(t)

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += float(dt)


class LinkStatsTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock(100.0)
        self.stats = bridge_health.LinkStats(clock=self.clock)
        self.names = {5: "swarm_pose", 6: "mavros/rc/in",
                      8: "trajectory_intent"}

    def test_remote_counts_and_gap_accounting(self):
        self.stats.note_rx("UAV8", 5)
        self.clock.advance(0.05)
        self.stats.note_rx("UAV8", 5)
        self.clock.advance(1.20)          # 一次 1.2 s 空档
        self.stats.note_rx("UAV8", 5)
        self.clock.advance(0.10)
        self.stats.note_rx("UAV8", 5)
        self.clock.advance(0.10)          # 距最后一帧 100 ms（age 口径）

        rows = self.stats.matrix(self.names, {5}, now=self.clock())
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((row["src"], row["rx"]), ("UAV8", 4))
        self.assertAlmostEqual(row["gap_max_ms"], 1200.0, places=1)
        self.assertEqual(row["gt"], {"gt200": 1, "gt500": 1, "gt1000": 1, "gt3000": 0})
        self.assertAlmostEqual(row["age_ms"], 100.0, places=1)

    def test_local_counts_include_throttle_drops(self):
        for _ in range(3):
            self.stats.note_local_rx(5)
        self.stats.note_tx(5)
        self.stats.note_local_drop(5)
        self.stats.note_local_drop(5)

        summary = self.stats.summary(self.names, {5}, now=self.clock())
        row = summary["local"][0]
        self.assertEqual((row["rx"], row["tx"], row["drop"]), (3, 1, 2))
        self.assertEqual(self.stats.totals(), (3, 1, 2, 0))

    def test_summary_reports_oldest_and_stale_sources(self):
        self.stats.note_rx("UAV8", 5)
        self.stats.note_rx("UAV9", 5)
        self.clock.advance(0.2)
        self.stats.note_rx("UAV9", 5)
        self.clock.advance(2.0)           # UAV8 已陈旧 2.2 s，UAV9 只有 2.0 s

        summary = self.stats.summary(self.names, {5}, now=self.clock(), stale_age_s=1.5)
        row = summary["remote"][0]
        self.assertEqual(row["topic"], "swarm_pose")
        self.assertEqual(row["srcs"], 2)
        self.assertEqual(row["rx"], 3)
        self.assertEqual(row["age_max_src"], "UAV8")
        self.assertGreater(row["age_max_ms"], 2000.0)
        self.assertEqual([item["src"] for item in row["stale_srcs"]], ["UAV8", "UAV9"])

    def test_summary_only_covers_key_topics(self):
        self.stats.note_rx("UAV8", 5)
        self.stats.note_rx("UAV8", 6)
        summary = self.stats.summary(self.names, {5}, now=self.clock())
        self.assertEqual([row["topic"] for row in summary["remote"]], ["swarm_pose"])
        self.assertEqual([row["topic"] for row in summary["local"]], [])

    def test_matrix_skips_non_key_topics(self):
        self.stats.note_rx("UAV8", 5)
        self.stats.note_rx("UAV8", 6)
        rows = self.stats.matrix(self.names, {5}, now=self.clock())
        self.assertEqual([row["topic"] for row in rows], ["swarm_pose"])

    def test_stall_events_start_then_end(self):
        self.stats.note_rx("UAV8", 5)
        self.clock.advance(0.05)
        self.stats.note_rx("UAV8", 5)

        self.clock.advance(2.0)                     # 静默 2 s > warn 1.5 s
        events = self.stats.poll_stalls(self.clock(), warn_s=1.5)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "stall_start")
        self.assertEqual((events[0]["scope"], events[0]["src"], events[0]["topic_idx"]),
                         ("remote", "UAV8", 5))
        self.assertGreater(events[0]["silent_s"], 1.5)

        self.clock.advance(1.0)                     # 仍是同一停顿：不重复报
        self.assertEqual(self.stats.poll_stalls(self.clock(), warn_s=1.5), [])

        self.clock.advance(0.1)
        self.stats.note_rx("UAV8", 5)               # 恢复
        events = self.stats.poll_stalls(self.clock(), warn_s=1.5)
        self.assertEqual([e["event"] for e in events], ["stall_end"])
        self.assertGreater(events[0]["duration_s"], 2.0)

    def test_never_seen_pair_is_not_a_stall(self):
        self.clock.advance(10.0)
        self.assertEqual(self.stats.poll_stalls(self.clock(), warn_s=1.5), [])

    def test_stall_filter_excludes_event_driven_topics(self):
        self.stats.note_rx("UAV8", 5)
        self.stats.note_rx("UAV8", 8)               # trajectory_intent
        self.clock.advance(3.0)
        events = self.stats.poll_stalls(self.clock(), warn_s=1.5,
                                        key_filter=lambda scope, src, idx: idx == 5)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["topic_idx"], 5)

    def test_warn_disabled_returns_no_events(self):
        self.stats.note_rx("UAV8", 5)
        self.clock.advance(10.0)
        self.assertEqual(self.stats.poll_stalls(self.clock(), warn_s=0.0), [])


class HealthWriterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bridge_health_test_")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_writes_jsonl_lines_with_timestamp(self):
        path = os.path.join(self.tmp, "nested", "UAV8_bridge_health.jsonl")
        writer = bridge_health.HealthWriter(path)
        writer.emit({"event": "sample", "age_ms": 12.0})
        writer.emit({"event": "stall_start", "silent_s": 2.5})
        writer.close()

        with open(path) as handle:
            lines = [json.loads(line) for line in handle if line.strip()]
        self.assertEqual([line["event"] for line in lines], ["sample", "stall_start"])
        self.assertTrue(all("t" in line for line in lines))
        self.assertEqual(writer.errors, 0)

    def test_appends_across_instances(self):
        path = os.path.join(self.tmp, "h.jsonl")
        first = bridge_health.HealthWriter(path)
        first.emit({"event": "a"})
        first.close()
        second = bridge_health.HealthWriter(path)
        second.emit({"event": "b"})
        second.close()
        with open(path) as handle:
            self.assertEqual([json.loads(line)["event"] for line in handle], ["a", "b"])

    def test_none_path_is_a_silent_noop(self):
        writer = bridge_health.HealthWriter(None)
        writer.emit({"event": "ignored"})           # 不应抛
        writer.close()

    def test_unwritable_path_warns_and_counts_errors(self):
        warnings = []
        writer = bridge_health.HealthWriter("/proc/definitely/not/writable/h.jsonl",
                                            warn=warnings.append)
        writer.emit({"event": "boom"})              # 内部吞掉异常
        writer.emit({"event": "boom-again"})        # 已禁用，同样不抛
        writer.close()
        self.assertEqual(writer.errors, 1)
        self.assertEqual(len(warnings), 1)


class ApplyLinkOptionsTest(unittest.TestCase):
    def _socket(self, kind):
        try:
            import zmq
        except ImportError:                          # pragma: no cover
            self.skipTest("pyzmq not installed")
        context = zmq.Context()
        sock = context.socket(getattr(zmq, kind))
        self.addCleanup(context.term)
        self.addCleanup(sock.close, 0)
        return zmq, sock

    def test_heartbeat_values_match_defaults(self):
        zmq, sock = self._socket("PUB")
        bridge_health.apply_link_options(sock)
        self.assertEqual(sock.getsockopt(zmq.HEARTBEAT_IVL), 1000)
        # 现场实测有 1~3 s 空档 → 超时取 10 s，避免高频拆链重建
        self.assertEqual(sock.getsockopt(zmq.HEARTBEAT_TIMEOUT), 10000)
        self.assertEqual(sock.getsockopt(zmq.TCP_KEEPALIVE), 1)
        self.assertEqual(sock.getsockopt(zmq.RECONNECT_IVL), 200)
        # 退避上限 1 s（跟随者 1.0 s 就判 LEADER_LOST，恢复不能更慢）
        self.assertEqual(sock.getsockopt(zmq.RECONNECT_IVL_MAX), 1000)
        if hasattr(zmq, "HEARTBEAT_TTL"):
            self.assertEqual(sock.getsockopt(zmq.HEARTBEAT_TTL), 0)

    def test_send_timeout_applied_for_dealer(self):
        zmq, sock = self._socket("DEALER")
        applied = bridge_health.apply_link_options(sock, sndtimeo_ms=500)
        self.assertIn("SNDTIMEO", applied)
        self.assertEqual(sock.getsockopt(zmq.SNDTIMEO), 500)

    def test_send_timeout_skipped_when_zero(self):
        zmq, sock = self._socket("SUB")
        applied = bridge_health.apply_link_options(sock)
        self.assertNotIn("SNDTIMEO", applied)


class ConnectionMonitorTest(unittest.TestCase):
    """端到端：真实 ZMQ 连接必须产生连接事件（pyzmq 给的 endpoint 是 bytes，
    不归一化会让 json 序列化抛错、事件被静默吞掉——这正是本次修掉的问题）。"""
    def test_reports_connect_events_with_text_endpoint(self):
        try:
            import zmq
        except ImportError:                          # pragma: no cover
            self.skipTest("pyzmq not installed")

        context = zmq.Context()
        pub = context.socket(zmq.PUB)
        sub = context.socket(zmq.SUB)
        self.addCleanup(context.term)
        self.addCleanup(pub.close, 0)
        self.addCleanup(sub.close, 0)
        port = pub.bind_to_random_port("tcp://127.0.0.1")

        events = []
        monitor = bridge_health.ConnectionMonitor(
            sub, lambda name, endpoint, value: events.append((name, endpoint, value)))
        self.addCleanup(monitor.close)
        sub.setsockopt(zmq.SUBSCRIBE, b"")
        sub.connect("tcp://127.0.0.1:%d" % port)

        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and not events:
            time.sleep(0.05)
        monitor.close()

        self.assertTrue(events, "未收到任何连接事件")
        name, endpoint, _value = events[0]
        self.assertIsInstance(name, str)
        self.assertIsInstance(endpoint, str)          # bytes 必须已归一化
        self.assertIn("tcp://127.0.0.1:%d" % port, endpoint)

    def test_monitor_event_name_covers_connect_family(self):
        try:
            import zmq
        except ImportError:                          # pragma: no cover
            self.skipTest("pyzmq not installed")
        for constant in ("EVENT_CONNECTED", "EVENT_CONNECT_DELAYED",
                         "EVENT_CONNECT_RETRIED", "EVENT_DISCONNECTED"):
            code = getattr(zmq, constant)
            self.assertFalse(bridge_health._monitor_event_name(code).startswith("code_"),
                             constant)


class MiscHelpersTest(unittest.TestCase):
    def test_loadavg_and_self_cpu_are_readable(self):
        self.assertIsNotNone(bridge_health.read_loadavg())
        self.assertIsNotNone(bridge_health.read_self_cpu_seconds())

    def test_monitor_event_names_map_to_known_constants(self):
        try:
            import zmq
        except ImportError:                          # pragma: no cover
            self.skipTest("pyzmq not installed")
        self.assertEqual(bridge_health._monitor_event_name(zmq.EVENT_CONNECTED), "connected")
        self.assertEqual(bridge_health._monitor_event_name(zmq.EVENT_DISCONNECTED), "disconnected")
        # pyzmq 18.1.1 只有 EVENT_CONNECT_RETRIED（没有 EVENT_RETRIED）
        self.assertEqual(bridge_health._monitor_event_name(zmq.EVENT_CONNECT_RETRIED),
                         "connect_retried")
        self.assertTrue(bridge_health._monitor_event_name(999999).startswith("code_"))


class BundleStatsTest(unittest.TestCase):
    """合包（packing=bundle）埋点：整源计数、summary.sources、note_local_drop(n)、bundle stall。"""

    def setUp(self):
        self.clock = FakeClock(100.0)
        self.stats = bridge_health.LinkStats(clock=self.clock)
        self.names = {5: "swarm_pose", 6: "mavros/rc/in"}

    def test_bundle_rx_counts_and_gap(self):
        self.stats.note_bundle_rx("UAV8", frames=1, msgs=4, nbytes=300)
        self.clock.advance(0.05)
        self.stats.note_bundle_rx("UAV8", frames=2, msgs=5, nbytes=400)  # 切了 2 帧
        self.clock.advance(1.20)                                          # 一次 1.2 s 整源空档
        self.stats.note_bundle_rx("UAV8", frames=1, msgs=3, nbytes=150)
        self.clock.advance(0.10)

        out = self.stats.summary(self.names, {5}, now=self.clock())
        self.assertEqual(len(out["sources"]), 1)
        s = out["sources"][0]
        self.assertEqual((s["src"], s["frames"], s["msgs"], s["bytes"]), ("UAV8", 4, 12, 850))
        self.assertAlmostEqual(s["gap_max_ms"], 1200.0, places=1)
        self.assertAlmostEqual(s["age_ms"], 100.0, places=1)
        self.assertGreaterEqual(s["gt"]["gt1000"], 1)

    def test_bundle_totals(self):
        self.stats.note_bundle_tx(frames=2, msgs=6, nbytes=900)
        self.stats.note_bundle_tx(frames=1, msgs=3, nbytes=450)
        self.stats.note_bundle_rx("UAV10", frames=3, msgs=9, nbytes=1200)
        frame_tx, msg_tx, byte_tx, frame_rx = self.stats.bundle_totals()
        self.assertEqual((frame_tx, msg_tx, byte_tx, frame_rx), (3, 9, 1350, 3))

    def test_note_local_drop_with_n(self):
        self.stats.note_local_drop(5, 3)
        self.stats.note_local_rx(5)
        self.stats.note_tx(5)
        out = self.stats.summary(self.names, {5}, now=self.clock())
        row = out["local"][0]
        self.assertEqual(row["drop"], 3)
        self.assertEqual(row["rx"], 1)

    def test_bundle_stall_scope(self):
        self.stats.note_bundle_rx("UAV12", frames=1, msgs=2, nbytes=100)
        self.clock.advance(2.0)                                          # 超过 warn_s
        events = self.stats.poll_stalls(self.clock(), warn_s=1.5,
                                        key_filter=lambda scope, src, idx: scope == "bundle")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["scope"], "bundle")
        self.assertEqual(events[0]["src"], "UAV12")


if __name__ == "__main__":
    unittest.main()
