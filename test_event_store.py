"""EventStore 持久化与重启恢复测试。"""

import tempfile
import unittest
from pathlib import Path

from event_store import EventStore
from migration_bus import HELD, MigrationBus
from replay import dispatch


class EventStoreTests(unittest.TestCase):
    def _events(self):
        return [
            {"timestamp": 0, "type": "subscribe",
             "data": {"group": "g", "device": "d", "channel": "old"}},
            {"timestamp": 1, "type": "event",
             "data": {"device": "d", "epoch": 1, "seq": 1, "channel": "old",
                      "transport": "o1", "body": {"step": "load"}}},
            {"timestamp": 2, "type": "poll",
             "data": {"group": "g", "device": "d", "member": "m1", "delivery": "x1"}},
            {"timestamp": 3, "type": "complete_epoch",
             "data": {"device": "d", "epoch": 1, "last_seq": 1, "gaps": []}},
        ]

    def test_journal_and_snapshot_recover_state(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(directory)
            bus = MigrationBus()
            applied = 0
            for event in self._events():
                store.append(event)
                dispatch(bus, event)
                applied += 1
            store.save_snapshot(bus, applied)
            store.close()

            # 模拟新进程：从快照恢复，未确认交付仍是在途。
            store2 = EventStore(directory)
            snapshot = store2.load_snapshot()
            reborn = MigrationBus.restore(snapshot["state"])
            report = reborn.inspect("d", "g")
            self.assertEqual(report["group"]["in_flight"][0]["status"], HELD)
            store2.close()

    def test_tail_events_after_snapshot_are_replayed(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(directory)
            bus = MigrationBus()
            events = self._events()
            for event in events[:3]:
                store.append(event)
                dispatch(bus, event)
            store.save_snapshot(bus, 3)

            # 快照之后又 fsync 了一条事件，但没来得及再快照就“崩溃”。
            store.append(events[3])
            store.close()

            store2 = EventStore(directory)
            snapshot = store2.load_snapshot()
            reborn = MigrationBus.restore(snapshot["state"])
            for event in store2.read_journal()[snapshot["applied_events"]:]:
                dispatch(reborn, event)
            epoch = reborn.devices["d"].epochs[1]
            self.assertEqual(epoch.last_seq, 1)
            store2.close()

    def test_journal_file_is_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(directory)
            for event in self._events():
                store.append(event)
            store.close()
            lines = Path(directory, "journal.jsonl").read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 4)


if __name__ == "__main__":
    unittest.main()
