#!/usr/bin/env python3
"""fleet_monitor.py — GCS-only terminal monitor for UAV fleet status.

Subscribes to the bridge-forwarded /UAVn/mavros/{state,battery,
gpsstatus/gps1/raw} topics and redraws a table at 1 Hz. Fields with no
update for ~stale_s seconds are shown as "--".
"""
from __future__ import annotations

import sys
import threading
import time

CLEAR_SCREEN = "\033[H\033[2J"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"
MODE_WIDTH = 16


class Snapshot:
    """Latest value plus receive time for each (uav, field)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}

    def update(self, uav, field, value, now=None):
        stamp = time.monotonic() if now is None else now
        with self._lock:
            self._values[(uav, field)] = (value, stamp)

    def display(self, uav, field, now, stale_s):
        with self._lock:
            item = self._values.get((uav, field))
        if item is None:
            return "--"
        value, stamp = item
        if now - stamp > stale_s:
            return "--"
        if field == "voltage":
            return "%.2f" % value
        if field == "fix_type":
            return str(int(value))
        return str(value)[:MODE_WIDTH]


def render_table(uavs, snapshot, now, stale_s):
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    lines = [
        "Fleet Status   %s   stale > %.1fs: --" % (stamp, stale_s),
        "%-8s%-13s%-16s%s" % ("UAV", "Voltage(V)", "Mode", "GPS Fix"),
    ]
    for uav in uavs:
        lines.append("%-8s%-13s%-16s%s" % (
            uav,
            snapshot.display(uav, "voltage", now, stale_s),
            snapshot.display(uav, "mode", now, stale_s),
            snapshot.display(uav, "fix_type", now, stale_s),
        ))
    return "\n".join(lines) + "\n"


def main():
    import rospy
    from mavros_msgs.msg import BatteryStatus, GPSRAW, State

    rospy.init_node("fleet_monitor")
    uav_count = int(rospy.get_param("~uav_count", 16))
    uav_prefix = str(rospy.get_param("~uav_prefix", "UAV"))
    refresh_hz = float(rospy.get_param("~refresh_hz", 1.0))
    stale_s = float(rospy.get_param("~stale_s", 3.0))

    snap = Snapshot()
    uavs = ["%s%d" % (uav_prefix, i) for i in range(1, uav_count + 1)]
    for index, uav in enumerate(uavs, start=1):
        base = "/%s%d/mavros/" % (uav_prefix, index)

        def on_state(msg, name=uav):
            snap.update(name, "mode", msg.mode)

        def on_battery(msg, name=uav):
            snap.update(name, "voltage", msg.voltage)

        def on_gps(msg, name=uav):
            snap.update(name, "fix_type", msg.fix_type)

        rospy.Subscriber(base + "state", State, on_state, queue_size=2)
        rospy.Subscriber(base + "battery", BatteryStatus, on_battery,
                         queue_size=2)
        rospy.Subscriber(base + "gpsstatus/gps1/raw", GPSRAW, on_gps,
                         queue_size=2)

    def restore_terminal():
        sys.stdout.write(SHOW_CURSOR)
        sys.stdout.flush()

    rospy.on_shutdown(restore_terminal)
    sys.stdout.write(HIDE_CURSOR)
    rate = rospy.Rate(refresh_hz)
    try:
        while not rospy.is_shutdown():
            sys.stdout.write(CLEAR_SCREEN +
                             render_table(uavs, snap, time.monotonic(), stale_s))
            sys.stdout.flush()
            rate.sleep()
    finally:
        restore_terminal()


if __name__ == "__main__":
    main()
