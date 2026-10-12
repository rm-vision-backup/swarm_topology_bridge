#!/usr/bin/env python3
"""bundle 纯单元测试（无 ROS Master、无网络，只需 stdlib）。

覆盖：帧编解码往返 / magic+version+截断校验、按 entry 边界切帧与 FLAG_MORE、
超大单条独占一帧、TopicBuffer 的 latest/queue 语义与丢弃计数、due-check（max_freq 0/1/5/10/12）。
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import bundle  # noqa: E402


class EncodeDecodeTest(unittest.TestCase):
    def test_roundtrip_single(self):
        entries = [(0, b"abc")]
        f = bundle.encode_frame(7, entries)
        seq, more, out = bundle.decode_frame(f)
        self.assertEqual(seq, 7)
        self.assertFalse(more)
        self.assertEqual(out, entries)

    def test_roundtrip_multi_and_empty_and_max_idx(self):
        entries = [(0, b""), (3, b"xyz"), (255, b"\x00\x01\x02")]
        f = bundle.encode_frame(2, entries)
        seq, more, out = bundle.decode_frame(f)
        self.assertEqual((seq, more), (2, False))
        self.assertEqual(out, entries)

    def test_bad_magic(self):
        f = bytearray(bundle.encode_frame(1, [(0, b"a")]))
        f[0] ^= 0xFF
        with self.assertRaises(bundle.BundleError) as cm:
            bundle.decode_frame(bytes(f))
        self.assertEqual(cm.exception.reason, "bad_magic")

    def test_bad_version(self):
        f = bytearray(bundle.encode_frame(1, [(0, b"a")]))
        f[1] = 99
        with self.assertRaises(bundle.BundleError) as cm:
            bundle.decode_frame(bytes(f))
        self.assertEqual(cm.exception.reason, "bad_version")

    def test_truncated_header(self):
        with self.assertRaises(bundle.BundleError) as cm:
            bundle.decode_frame(b"\x5a")
        self.assertEqual(cm.exception.reason, "truncated")

    def test_truncated_payload(self):
        f = bundle.encode_frame(1, [(0, b"abcdef")])
        with self.assertRaises(bundle.BundleError) as cm:
            bundle.decode_frame(f[:-2])
        self.assertEqual(cm.exception.reason, "truncated")


class BuilderTest(unittest.TestCase):
    def test_single_frame(self):
        b = bundle.BundleBuilder(max_bytes=1400)
        entries = [(0, b"x" * 100), (1, b"y" * 100)]
        frames = b.build(0, entries)
        self.assertEqual(len(frames), 1)
        self.assertEqual(bundle.decode_frames(frames), entries)

    def test_split_on_size_and_more_flag(self):
        b = bundle.BundleBuilder(max_bytes=100)
        entries = [(0, b"a" * 40), (1, b"b" * 40), (2, b"c" * 40)]
        frames = b.build(5, entries)
        self.assertGreater(len(frames), 1)
        for f in frames:                       # 同一 tick 共享 seq
            self.assertEqual(bundle.decode_frame(f)[0], 5)
        self.assertTrue(bundle.decode_frame(frames[0])[1])    # 非末帧 MORE=1
        self.assertFalse(bundle.decode_frame(frames[-1])[1])  # 末帧 MORE=0
        self.assertEqual(bundle.decode_frames(frames), entries)

    def test_oversize_entry_own_frame(self):
        b = bundle.BundleBuilder(max_bytes=100)
        entries = [(0, b"z" * 500)]            # 单条自身 > max_bytes
        frames = b.build(0, entries)
        self.assertEqual(len(frames), 1)
        self.assertEqual(bundle.decode_frames(frames), entries)

    def test_empty_tick(self):
        b = bundle.BundleBuilder()
        self.assertEqual(b.build(0, []), [])


class TopicBufferTest(unittest.TestCase):
    def test_latest_keeps_newest_and_counts_overwrites(self):
        tb = bundle.TopicBuffer(0, "latest", 0, 10.0)
        tb.add(b"a")
        tb.add(b"b")
        tb.add(b"c")
        self.assertEqual(tb.pending(), 1)
        self.assertEqual(tb.take_due(0), [b"c"])
        self.assertEqual(tb.dropped(), 2)
        self.assertEqual(tb.take_due(1), [])   # 已清空

    def test_latest_not_due_keeps_value(self):
        tb = bundle.TopicBuffer(0, "latest", 5, 10.0)   # 每 2 tick
        tb.add(b"a")
        self.assertEqual(tb.take_due(1), [])            # tick1 未到 due
        self.assertEqual(tb.pending(), 1)               # 值保留
        self.assertEqual(tb.take_due(2), [b"a"])        # tick2 due

    def test_queue_drains_all(self):
        tb = bundle.TopicBuffer(0, "queue", 0, 10.0)
        for p in (b"1", b"2", b"3"):
            tb.add(p)
        self.assertEqual(tb.take_due(0), [b"1", b"2", b"3"])
        self.assertEqual(tb.dropped(), 0)
        self.assertEqual(tb.take_due(1), [])

    def test_queue_overflow_drops_oldest(self):
        tb = bundle.TopicBuffer(0, "queue", 0, 10.0, queue_max=2)
        for p in (b"1", b"2", b"3"):
            tb.add(p)
        self.assertEqual(tb.dropped(), 1)
        self.assertEqual(tb.take_due(0), [b"2", b"3"])

    def test_due_check_rates(self):
        # tick=10：max_freq 0/10/12 → 每 tick；5 → 每 2 tick；1 → 每 10 tick
        cases = [
            (0,  {0: True, 1: True, 2: True}),
            (10, {0: True, 1: True, 2: True}),
            (12, {0: True, 1: True, 2: True}),
            (5,  {0: True, 1: False, 2: True, 3: False, 4: True}),
            (1,  {0: True, 1: False, 5: False, 10: True, 11: False}),
        ]
        for mf, expect in cases:
            tb = bundle.TopicBuffer(0, "latest", mf, 10.0)
            for tick, should in expect.items():
                tb.add(b"v")
                self.assertEqual(bool(tb.take_due(tick)), should,
                                 "max_freq=%s tick=%s" % (mf, tick))


if __name__ == "__main__":
    unittest.main()
