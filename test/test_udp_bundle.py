#!/usr/bin/env python3
"""udp_bundle.py 的纯 stdlib 单测（无 ROS）。"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import pytest  # noqa: E402

from bundle import decode_frame  # noqa: E402
from bridge_health import LinkStats  # noqa: E402
from udp_bundle import (BundleReassembler, MAX_UDP_PAYLOAD,  # noqa: E402
                        UdpBundleSocket, build_udp_peer_maps)


def test_peer_maps_sim_same_ip_different_ports():
    uav_ip = {"UAV1": "127.0.0.1", "UAV2": "127.0.0.1", "UAV3": "127.0.0.1"}
    offsets = {"UAV1": 100, "UAV2": 200, "UAV3": 300}
    topology = {"UAV1": ["UAV2", "UAV3"], "UAV2": ["UAV1"], "UAV3": ["UAV1"]}
    local, addr_to_src, dsts = build_udp_peer_maps(
        "UAV1", uav_ip, offsets, topology, ["UAV2", "UAV3"], 4200)
    assert local == ("127.0.0.1", 4300)
    assert addr_to_src[("127.0.0.1", 4400)] == "UAV2"
    assert addr_to_src[("127.0.0.1", 4500)] == "UAV3"
    assert dsts == [("UAV2", ("127.0.0.1", 4400)), ("UAV3", ("127.0.0.1", 4500))]


def test_peer_maps_real_distinct_ips():
    uav_ip = {"UAV1": "192.168.5.11", "UAV2": "192.168.5.21"}
    offsets = {"UAV1": 0, "UAV2": 0}
    topology = {"UAV1": ["UAV2"], "UAV2": ["UAV1"]}
    local, addr_to_src, dsts = build_udp_peer_maps(
        "UAV2", uav_ip, offsets, topology, ["UAV1"], 4200)
    assert local == ("192.168.5.21", 4200)
    assert addr_to_src == {("192.168.5.11", 4200): "UAV1"}
    assert dsts == [("UAV1", ("192.168.5.11", 4200))]


def test_peer_maps_duplicate_address_raises():
    uav_ip = {"UAV1": "127.0.0.1", "UAV2": "127.0.0.1"}
    offsets = {"UAV1": 0, "UAV2": 0}
    topology = {"UAV1": ["UAV2"], "UAV2": ["UAV1"]}
    with pytest.raises(ValueError):
        build_udp_peer_maps("UAV1", uav_ip, offsets, topology, ["UAV2"], 4200)


def test_reassembler_single_frame():
    r = BundleReassembler()
    entries, discarded = r.feed(1, False, [(0, b"a"), (1, b"b")], 20)
    assert discarded == 0
    assert entries == [(0, b"a"), (1, b"b")]
    assert r.incomplete_ticks == 0


def test_reassembler_multi_frame():
    r = BundleReassembler()
    out, _ = r.feed(5, True, [(0, b"a")], 10)
    assert out is None
    out, _ = r.feed(5, True, [(1, b"b")], 10)
    assert out is None
    out, _ = r.feed(5, False, [(2, b"c")], 10)
    assert out == [(0, b"a"), (1, b"b"), (2, b"c")]


def test_reassembler_new_seq_discards_incomplete():
    r = BundleReassembler()
    r.feed(7, True, [(0, b"a")], 10)
    out, discarded = r.feed(8, False, [(1, b"b")], 10)
    assert discarded == 1
    assert r.incomplete_ticks == 1
    assert out == [(1, b"b")]


def test_reassembler_ignores_duplicate_completed_tick():
    r = BundleReassembler()
    assert r.feed(9, False, [(0, b"a")], 10)[0] == [(0, b"a")]
    assert r.feed(9, False, [(0, b"a")], 10)[0] is None


def test_udp_loopback_roundtrip():
    a = UdpBundleSocket(("127.0.0.1", 0))
    b = UdpBundleSocket(("127.0.0.1", 0))
    try:
        frame = b"\x5a\x01\x00\x00\x00\x00\x00\x00\x00"   # 空 tick 的合法头
        counts = a.send_frames([frame], [("B", b.bound_addr)])
        assert counts == {"datagrams": 1, "errors": 0, "oversize": 0, "blocked": 0}
        batch = b.recv_batch(timeout=1.0)
        assert len(batch) == 1
        data, addr, truncated = batch[0]
        assert data == frame
        assert addr == a.bound_addr
        assert not truncated
        seq, more, entries = decode_frame(data)
        assert seq == 0 and more is False and entries == []
    finally:
        a.close()
        b.close()


def test_udp_oversize_is_counted_not_sent():
    a = UdpBundleSocket(("127.0.0.1", 0))
    b = UdpBundleSocket(("127.0.0.1", 0))
    try:
        counts = a.send_frames([b"x" * (MAX_UDP_PAYLOAD + 1)], [("B", b.bound_addr)])
        assert counts["oversize"] == 1
        assert counts["datagrams"] == 0
        assert b.recv_batch(timeout=0.2) == []
    finally:
        a.close()
        b.close()


def test_linkstats_udp_counters_and_incomplete():
    stats = LinkStats(clock=lambda: 100.0)
    stats.note_bundle_rx("UAV2", 1, 2, 100)
    stats.note_udp_incomplete("UAV2", 2)
    stats.note_udp_tx(datagrams=6, errors=1, oversize=2, blocked=3)
    stats.note_udp_rx_anomaly("unknown", 4)
    stats.note_udp_rx_anomaly("decode", 5)
    stats.note_udp_rx_anomaly("truncated", 6)
    summary = stats.summary({}, set(), now=100.0)
    row = [r for r in summary["sources"] if r["src"] == "UAV2"][0]
    assert row["incomplete"] == 2
    assert stats.udp_summary() == {
        "tx_datagrams": 6, "tx_errors": 1, "tx_oversize": 2, "tx_blocked": 3,
        "rx_unknown": 4, "rx_decode_errors": 5, "rx_truncated": 6,
    }
