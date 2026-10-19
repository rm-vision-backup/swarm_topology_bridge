#!/usr/bin/env python3
"""fleet_monitor 纯渲染逻辑单测（不依赖 ROS）。"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "scripts"))

import fleet_monitor  # noqa: E402


def test_row_formats_voltage_mode_and_fix_type():
    snap = fleet_monitor.Snapshot()
    snap.update("UAV1", "voltage", 12.345, now=100.0)
    snap.update("UAV1", "mode", "OFFBOARD", now=100.0)
    snap.update("UAV1", "fix_type", 6, now=100.0)
    text = fleet_monitor.render_table(["UAV1"], snap, now=100.0, stale_s=3.0)
    assert "12.35" in text
    assert "OFFBOARD" in text
    assert "6" in text


def test_stale_field_becomes_dash_independently():
    snap = fleet_monitor.Snapshot()
    snap.update("UAV2", "voltage", 11.9, now=100.0)
    snap.update("UAV2", "mode", "POSCTL", now=104.0)
    snap.update("UAV2", "fix_type", 3, now=104.0)
    text = fleet_monitor.render_table(["UAV2"], snap, now=104.0, stale_s=3.0)
    row = [line for line in text.splitlines() if line.startswith("UAV2")][0]
    assert row.split()[1] == "--"
    assert "POSCTL" in row
    assert row.split()[-1] == "3"


def test_missing_field_is_dash():
    snap = fleet_monitor.Snapshot()
    text = fleet_monitor.render_table(["UAV3"], snap, now=100.0, stale_s=3.0)
    row = [line for line in text.splitlines() if line.startswith("UAV3")][0]
    assert row.split()[1:] == ["--", "--", "--"]
