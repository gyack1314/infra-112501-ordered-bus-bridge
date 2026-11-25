"""MigrationBus 规则单元测试：去重、冲突、空号、代次、切换、持有权与下线。"""

import threading
import unittest

from migration_bus import (
    CLOSED,
    HELD,
    MigrationBus,
    NEW,
    OLD,
    ProtocolViolation,
)


class BusTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.bus = MigrationBus()
        self.t = 0

    def tick(self) -> int:
        self.t += 1
        return self.t

    def emit(self, device, epoch, seq, channel, transport, body):
        return self.bus.ingest(device, epoch, seq, channel, transport, body, self.tick())

    def complete(self, device, epoch, last_seq, gaps=None):
        return self.bus.declare_complete(device, epoch, last_seq, gaps or [], self.tick())

    def poll(self, group, device, member, delivery_id):
        return self.bus.poll(group, device, member, delivery_id, self.tick())

    def ack(self, group, device, member, delivery_id):
        return self.bus.ack(group, device, member, delivery_id, self.tick())


class IngestTests(BusTestBase):
    def test_dual_write_same_body_enters_once(self):
        r1 = self.emit("d", 1, 1, OLD, "o1", {"step": "open"})
        r2 = self.emit("d", 1, 1, NEW, "n1", {"step": "open"})
        self.assertEqual(r1["outcome"], "added")
        self.assertEqual(r2["outcome"], "deduped")
        position = self.bus.devices["d"].epochs[1].positions[1]
        self.assertEqual(len(position.records), 2)
        self.assertFalse(position.conflict)

    def test_retry_same_transport_same_body_is_dedup(self):
        self.emit("d", 1, 1, OLD, "o1", {"step": "open"})
        again = self.emit("d", 1, 1, OLD, "o1", {"step": "open"})
        self.assertEqual(again["outcome"], "deduped")

    def test_same_transport_different_body_violates(self):
        self.emit("d", 1, 1, OLD, "o1", {"step": "open"})
        with self.assertRaises(ProtocolViolation):
            self.emit("d", 1, 1, OLD, "o1", {"step": "close"})

    def test_distinct_bodies_keep_conflict_evidence(self):
        self.emit("d", 1, 1, OLD, "o1", {"step": "open"})
        conflict = self.emit("d", 1, 1, NEW, "n1", {"step": "close"})
        self.assertEqual(conflict["outcome"], "conflicted")
        position = self.bus.devices["d"].epochs[1].positions[1]
        self.assertTrue(position.conflict)
        self.assertEqual(set(position.records), {"o1", "n1"})

    def test_conflict_blocks_delivery_until_resolved(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"step": "open"})
        self.emit("d", 1, 1, NEW, "n1", {"step": "close"})
        self.assertEqual(self.poll("g", "d", "m", "x1")["result"], "conflict")
        self.bus.resolve("d", 1, 1, "n1", self.tick())
        reply = self.poll("g", "d", "m", "x2")
        # n1 只在 new，旧组须先切换；先验证冲突已解除。
        self.assertIn(reply["result"], ("awaiting_channel", "delivered"))

    def test_resolve_unknown_transport_rejected(self):
        self.emit("d", 1, 1, OLD, "o1", {"step": "open"})
        self.emit("d", 1, 1, NEW, "n1", {"step": "close"})
        with self.assertRaises(ProtocolViolation):
            self.bus.resolve("d", 1, 1, "ghost", self.tick())

    def test_post_delivery_conflict_only_keeps_evidence(self):
        self.bus.subscribe("g", "d", NEW, 0)
        self.emit("d", 1, 1, NEW, "n1", {"step": "open"})
        reply = self.poll("g", "d", "m", "x1")
        self.assertEqual(reply["result"], "delivered")
        transport = reply["delivery"]["transport"]
        # 交付后不同内容到达：留证但不改变已交付内容。
        self.emit("d", 1, 1, OLD, "o9", {"step": "different"})
        position = self.bus.devices["d"].epochs[1].positions[1]
        self.assertTrue(position.delivered)
        self.assertEqual(position.delivered_transport, transport)
        self.assertIn("o9", position.post_delivery_conflicts)
        with self.assertRaises(ProtocolViolation):
            self.bus.resolve("d", 1, 1, "o9", self.tick())


class GapAndEpochTests(BusTestBase):
    def test_gap_blocks_later_positions_until_declared(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 3, OLD, "o3", {"v": 3})
        first = self.poll("g", "d", "m", "x1")
        self.assertEqual(first["delivery"]["seq"], 1)
        self.ack("g", "d", "m", "x1")
        waiting = self.poll("g", "d", "m", "x2")
        self.assertEqual(waiting["result"], "gap_pending")
        self.assertEqual(waiting["waiting"], {"epoch": 1, "seq": 2})

    def test_declared_gap_is_skipped(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 3, OLD, "o3", {"v": 3})
        self.complete("d", 1, 3, [[2, 2]])
        self.poll("g", "d", "m", "x1")
        self.ack("g", "d", "m", "x1")
        reply = self.poll("g", "d", "m", "x2")
        self.assertEqual(reply["result"], "delivered")
        self.assertEqual(reply["delivery"]["seq"], 3)

    def test_gap_over_received_content_rejected(self):
        self.emit("d", 1, 2, OLD, "o2", {"v": 2})
        with self.assertRaises(ProtocolViolation):
            self.complete("d", 1, 3, [[1, 2]])

    def test_missing_number_in_declared_epoch_blocks_retire(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 3, OLD, "o3", {"v": 3})
        self.complete("d", 1, 3, [])  # seq 2 既无内容也未报空
        result = self.bus.retire("d", 1, 3, self.tick())
        self.assertEqual(result["outcome"], "blocked")
        self.assertIn({"kind": "missing_event", "epoch": 1, "seq": 2}, result["blockers"])
        # 迟到事件补齐后，缺号阻塞解除。
        self.emit("d", 1, 2, OLD, "o2", {"v": 2})
        self.assertNotIn(
            "missing_event",
            [b["kind"] for b in self.bus.retire("d", 1, 3, self.tick())["blockers"]],
        )

    def test_new_epoch_blocked_until_old_closed(self):
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        with self.assertRaises(ProtocolViolation):
            self.emit("d", 2, 1, NEW, "n1", {"v": 1})
        self.complete("d", 1, 1)
        ok = self.emit("d", 2, 1, NEW, "n1", {"v": 1})
        self.assertEqual(ok["outcome"], "added")

    def test_event_beyond_declared_last_seq_rejected(self):
        self.complete("d", 1, 2, [[2, 2]])
        with self.assertRaises(ProtocolViolation):
            self.emit("d", 1, 3, OLD, "o3", {"v": 3})

    def test_event_arriving_after_gap_declaration_rejected(self):
        # 空号位置按定义没有事件；声明之后送到该位置的内容不能推翻空号。
        self.complete("d", 1, 2, [[2, 2]])
        with self.assertRaises(ProtocolViolation):
            self.emit("d", 1, 2, OLD, "o2", {"v": 2})

    def test_event_arriving_after_completion_within_range_accepted(self):
        # 未报空的位置，完结声明之后迟到的事件仍然接纳。
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.complete("d", 1, 2, [[2, 2]])
        late = self.emit("d", 1, 1, NEW, "n1", {"v": 1})
        self.assertEqual(late["outcome"], "deduped")

    def test_redeclaration_must_match(self):
        self.complete("d", 1, 2, [[2, 2]])
        self.assertEqual(self.complete("d", 1, 2, [[2, 2]])["outcome"], "unchanged")
        with self.assertRaises(ProtocolViolation):
            self.complete("d", 1, 2, [])

    def test_devices_advance_independently(self):
        self.emit("a", 1, 1, OLD, "a1", {"v": 1})
        # b 的代次 1 未完结不影响 a 的代次 2。
        self.emit("b", 1, 1, OLD, "b1", {"v": 1})
        self.complete("a", 1, 1)
        ok = self.emit("a", 2, 1, NEW, "a2", {"v": 1})
        self.assertEqual(ok["outcome"], "added")

    def test_subscribe_before_events_aligns_to_first_real_epoch(self):
        # 订阅时设备还没有任何代次，随后数据从 epoch 11 开始（资料场景）。
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 11, 1, OLD, "o1", {"v": 1})
        reply = self.poll("g", "d", "m", "x1")
        self.assertEqual(reply["result"], "delivered")
        self.assertEqual(reply["delivery"]["epoch"], 11)
        self.assertEqual(self.bus.subscriptions[("g", "d")].cursor_epoch, 11)

    def test_poll_on_empty_device_waits(self):
        self.bus.subscribe("g", "d", OLD, 0)
        reply = self.poll("g", "d", "m", "x1")
        self.assertEqual(reply["result"], "gap_pending")


class DeliveryAndGroupTests(BusTestBase):
    def _feed(self, device="d", epoch=1, seq=1, channel=OLD, transport="o1", body=None):
        return self.emit(device, epoch, seq, channel, transport, body or {"v": seq})

    def test_second_poll_while_held_reports_in_flight(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self._feed()
        self.poll("g", "d", "m1", "x1")
        reply = self.poll("g", "d", "m1", "x2")
        self.assertEqual(reply["result"], "in_flight")
        self.assertEqual(reply["held_by"], "m1")

    def test_expired_delivery_is_taken_over_by_another_member(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self._feed()
        self.poll("g", "d", "m1", "x1")
        self.bus.expire("g", "d", "m1", "x1", self.tick())
        reply = self.poll("g", "d", "m2", "x2")
        self.assertEqual(reply["result"], "redelivered")
        self.assertEqual(reply["delivery"]["member"], "m2")
        # 旧持有者不能确认新交付。
        stale = self.ack("g", "d", "m1", "x2")
        self.assertEqual(stale["outcome"], "rejected_not_holder")
        # 旧 delivery 的迟到确认同样被拒。
        late = self.ack("g", "d", "m1", "x1")
        self.assertEqual(late["outcome"], "rejected_stale")
        self.assertEqual(self.ack("g", "d", "m2", "x2")["outcome"], "acked")

    def test_giveback_allows_redelivery(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self._feed()
        self.poll("g", "d", "m1", "x1")
        self.assertEqual(
            self.bus.giveback("g", "d", "m1", "x1", self.tick())["outcome"], "giveback"
        )
        reply = self.poll("g", "d", "m2", "x2")
        self.assertEqual(reply["result"], "redelivered")

    def test_unknown_delivery_ack_violates(self):
        self.bus.subscribe("g", "d", OLD, 0)
        with self.assertRaises(ProtocolViolation):
            self.ack("g", "d", "m1", "ghost")

    def test_duplicate_delivery_id_rejected(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self._feed()
        self.poll("g", "d", "m1", "dup")
        with self.assertRaises(ProtocolViolation):
            self.poll("g", "d", "m1", "dup")

    def test_unsubscribe_closes_held_delivery(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self._feed()
        self.poll("g", "d", "m1", "x1")
        self.bus.unsubscribe("g", "d", self.tick())
        delivery = self.bus.deliveries[("g", "d")]["x1"]
        self.assertEqual(delivery.status, CLOSED)

    def test_resubscribe_starts_without_progress(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self._feed()
        self.poll("g", "d", "m1", "x1")
        self.ack("g", "d", "m1", "x1")
        self.bus.unsubscribe("g", "d", self.tick())
        self.bus.subscribe("g", "d", OLD, self.tick())
        reply = self.poll("g", "d", "m2", "x2")
        self.assertEqual(reply["result"], "delivered")
        self.assertEqual(reply["delivery"]["seq"], 1)


class SwitchTests(BusTestBase):
    def test_switch_waits_for_completable_old_in_flight(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.complete("d", 1, 1)
        self.poll("g", "d", "m1", "x1")
        reply = self.bus.request_switch("g", "d", 1, 1, self.tick())
        self.assertEqual(reply["outcome"], "waiting_in_flight")
        # 过期后没有仍可完成的旧在途，下一次交互自动完成切换。
        self.bus.expire("g", "d", "m1", "x1", self.tick())
        poll = self.poll("g", "d", "m2", "x2")
        sub = self.bus.subscriptions[("g", "d")]
        self.assertEqual(sub.channel, NEW)
        self.assertFalse(sub.switch_pending)
        # 未确认位置从新流补投，哪怕就在原定交接点上。
        self.assertEqual(poll["delivery"]["seq"], 1)
        self.assertEqual(poll["delivery"]["channel"], NEW)

    def test_switch_immediate_without_in_flight(self):
        self.bus.subscribe("g", "d", OLD, 0)
        reply = self.bus.request_switch("g", "d", 1, 0, self.tick())
        self.assertEqual(reply["outcome"], "switched")
        self.assertEqual(self.bus.subscriptions[("g", "d")].channel, NEW)

    def test_new_group_never_waits_for_old_group(self):
        self.bus.subscribe("oldg", "d", OLD, 0)
        self.bus.subscribe("newg", "d", NEW, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 1, NEW, "n1", {"v": 1})
        # oldg 持有旧在途且未切换，newg 照常消费。
        self.poll("oldg", "d", "m", "x1")
        fresh = self.poll("newg", "d", "m", "y1")
        self.assertEqual(fresh["delivery"]["channel"], NEW)

    def test_late_old_ack_after_finalize_rejected(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.poll("g", "d", "m1", "x1")
        self.bus.request_switch("g", "d", 1, 1, self.tick())
        self.bus.giveback("g", "d", "m1", "x1", self.tick())
        # giveback 后无 held，poll 触发切换完成并从新流补投。
        self.emit("d", 1, 1, NEW, "n1", {"v": 1})
        self.poll("g", "d", "m2", "x2")
        stale = self.ack("g", "d", "m1", "x1")
        self.assertEqual(stale["outcome"], "rejected_stale")

    def test_old_group_cannot_read_new_only_transport(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, NEW, "n1", {"v": 1})
        reply = self.poll("g", "d", "m1", "x1")
        self.assertEqual(reply["result"], "awaiting_channel")


class RetireTests(BusTestBase):
    def test_retire_blocked_by_open_producer_range(self):
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        result = self.bus.retire("d", 1, 1, self.tick())
        self.assertEqual(result["blockers"][0]["kind"], "producer_open")

    def test_retire_blocked_by_group_still_on_old(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.complete("d", 1, 1)
        result = self.bus.retire("d", 1, 1, self.tick())
        self.assertTrue(any(b["kind"] == "group_old_channel" for b in result["blockers"]))

    def test_retire_succeeds_after_switch_and_cursor_past_through(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 1, NEW, "n1", {"v": 1})
        self.complete("d", 1, 1)
        self.poll("g", "d", "m1", "x1")
        self.ack("g", "d", "m1", "x1")
        self.bus.request_switch("g", "d", 1, 1, self.tick())
        result = self.bus.retire("d", 1, 1, self.tick())
        self.assertEqual(result["outcome"], "retired")
        with self.assertRaises(ProtocolViolation):
            self.emit("d", 1, 2, OLD, "o2", {"v": 2})

    def test_retire_allowed_after_unsubscribe(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.complete("d", 1, 1)
        self.bus.unsubscribe("g", "d", self.tick())
        self.assertEqual(self.bus.retire("d", 1, 1, self.tick())["outcome"], "retired")

    def test_conflict_blocks_retire(self):
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 1, NEW, "n1", {"v": 2})
        self.complete("d", 1, 1)
        result = self.bus.retire("d", 1, 1, self.tick())
        self.assertTrue(any(b["kind"] == "producer_conflict" for b in result["blockers"]))

    def test_resolved_conflict_allows_retire(self):
        self.bus.subscribe("g", "d", NEW, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 1, NEW, "n1", {"v": 2})
        self.complete("d", 1, 1)
        self.bus.resolve("d", 1, 1, "n1", self.tick())
        result = self.bus.retire("d", 1, 1, self.tick())
        self.assertEqual(result["outcome"], "retired")

    def test_retire_is_idempotent_for_same_through(self):
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.complete("d", 1, 1)
        first = self.bus.retire("d", 1, 1, self.tick())
        second = self.bus.retire("d", 1, 1, self.tick())
        self.assertEqual(first["outcome"], "retired")
        self.assertTrue(second.get("idempotent"))
        with self.assertRaises(ProtocolViolation):
            self.bus.retire("d", 1, 2, self.tick())


class InspectClassificationTests(BusTestBase):
    def test_classifies_old_delivery_completable(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.poll("g", "d", "m", "x1")
        report = self.bus.inspect("d", "g")
        self.assertEqual(report["group"]["classification"], "old_delivery_completable")

    def test_classifies_missing_event(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 3, OLD, "o3", {"v": 3})
        self.complete("d", 1, 3, [])
        self.poll("g", "d", "m", "x1")
        self.ack("g", "d", "m", "x1")
        self.assertEqual(
            self.bus.inspect("d", "g")["group"]["classification"], "event_missing"
        )

    def test_classifies_producer_conflict(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 1, NEW, "n1", {"v": 2})
        self.assertEqual(
            self.bus.inspect("d", "g")["group"]["classification"], "producer_conflict"
        )

    def test_classifies_catching_up(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.assertEqual(
            self.bus.inspect("d", "g")["group"]["classification"], "catching_up"
        )


class ConcurrencyTests(BusTestBase):
    def test_concurrent_workers_deliver_each_position_once(self):
        self.bus.subscribe("g", "d", OLD, 0)
        for seq in range(1, 51):
            self.emit("d", 1, seq, OLD, f"o{seq}", {"v": seq})
        self.complete("d", 1, 50)
        delivered: list[tuple[int, str]] = []
        lock = threading.Lock()

        def worker(member: str) -> None:
            local = []
            for i in range(25):
                delivery_id = f"{member}-{i}"
                reply = self.bus.poll("g", "d", member, delivery_id, 100 + i)
                if reply["result"] == "delivered":
                    local.append((reply["delivery"]["seq"], delivery_id))
                    self.bus.ack("g", "d", member, delivery_id, 200 + i)
            with lock:
                delivered.extend(local)

        threads = [threading.Thread(target=worker, args=(f"m{k}",)) for k in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        seqs = [seq for seq, _ in delivered]
        self.assertEqual(len(seqs), 50)
        self.assertEqual(len(set(seqs)), 50)


class SnapshotTests(BusTestBase):
    def test_snapshot_restore_keeps_unacked_delivery(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.poll("g", "d", "m1", "x1")
        restored = MigrationBus.restore(self.bus.snapshot())
        report = restored.inspect("d", "g")
        self.assertEqual(len(report["group"]["in_flight"]), 1)
        self.assertEqual(report["group"]["in_flight"][0]["status"], HELD)
        # 重启后新成员接回并确认。
        restored.expire("g", "d", "m1", "x1", 99)
        reply = restored.poll("g", "d", "m2", "x2", 100)
        self.assertEqual(reply["result"], "redelivered")
        self.assertEqual(restored.ack("g", "d", "m2", "x2", 101)["outcome"], "acked")

    def test_snapshot_roundtrip_is_equivalent(self):
        self.bus.subscribe("g", "d", OLD, 0)
        self.emit("d", 1, 1, OLD, "o1", {"v": 1})
        self.emit("d", 1, 1, NEW, "n1", {"v": 1})
        self.complete("d", 1, 2, [[2, 2]])
        self.poll("g", "d", "m", "x1")
        import json

        restored = MigrationBus.restore(json.loads(json.dumps(self.bus.snapshot())))
        self.assertEqual(
            json.dumps(restored.snapshot(), sort_keys=True),
            json.dumps(self.bus.snapshot(), sort_keys=True),
        )


if __name__ == "__main__":
    unittest.main()
