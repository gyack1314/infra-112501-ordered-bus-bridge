"""端到端演练：用新旧适配方在本地跑通双写、切换、下线与重启。

运行：python3 migration_demo.py
脚本对每一步的预期结果都有断言，任何规则被违反都会直接失败。
"""

from __future__ import annotations

import json
import tempfile

from channel_adapters import (
    ConsumerGroupAdapter,
    CoordinatorAdapter,
    LogicalClock,
    ProducerAdapter,
)
from event_store import EventStore
from migration_bus import (
    HELD,
    MigrationBus,
    NEW,
    OLD,
    ProtocolViolation,
)


def show(title: str, payload: object) -> None:
    print(f"\n--- {title} ---")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    clock = LogicalClock()
    bus = MigrationBus()
    old_producer = ProducerAdapter(bus, OLD, clock)
    new_producer = ProducerAdapter(bus, NEW, clock)
    coordinator = CoordinatorAdapter(bus, clock)
    slow = ConsumerGroupAdapter(bus, "slow", clock)
    fresh = ConsumerGroupAdapter(bus, "fresh", clock)

    # 1. 两个设备独立推进，先看 botA。 ------------------------------------------------
    slow.subscribe("botA", OLD)
    fresh.subscribe("botA", NEW)

    # 双写：同内容不同 transport，只进一次。
    r1 = old_producer.emit("botA", 1, 1, "o-1", {"step": "load"})
    r2 = new_producer.emit("botA", 1, 1, "n-1", {"step": "load"})
    expect(r1["outcome"] == "added" and r2["outcome"] == "deduped", "双写同内容应去重")

    # 同 transport 重试也是同一业务事件。
    r3 = old_producer.emit("botA", 1, 1, "o-1", {"step": "load"})
    expect(r3["outcome"] == "deduped", "同传输标识重试应去重")

    # 同位置不同内容：保留冲突证据，位置卡住。
    new_producer.emit("botA", 1, 2, "n-2", {"step": "work"})
    old_producer.emit("botA", 1, 2, "o-2", {"step": "work-other"})
    conflicted = bus.inspect("botA")["summary"]["producer_conflicts"]
    expect(conflicted == [{"epoch": 1, "seq": 2}], f"应识别生产端冲突: {conflicted}")

    # 缺口：seq 3 未到，后面的 seq 4 可以先接纳但不能越过缺口交付。
    old_producer.emit("botA", 1, 4, "o-4", {"step": "unload"})
    poll = slow.poll("botA", "m1", "d1")
    expect(poll["result"] == "delivered" and poll["delivery"]["seq"] == 1, "首位置应可交付")
    slow.ack("botA", "m1", "d1")
    blocked = slow.poll("botA", "m1", "d2")
    expect(blocked["result"] == "conflict", f"冲突位未裁定前必须卡住: {blocked}")

    # 生产方只针对这一个位置裁定有效副本。
    new_producer.resolve("botA", 1, 2, "n-2")
    poll = slow.poll("botA", "m1", "d2")
    # 旧组拿不到只存在于新流的内容，需先切换。
    expect(poll["result"] == "awaiting_channel", f"旧组不应直接读新流副本: {poll}")

    # 2. 切换：旧在途排空后才落到新流，新流从第一个未确认位置接续。 ------------------
    # 先让旧组在 old 上持有一个交付，再申请切换，应处于等待。
    # seq 2 的有效副本在 new，old 侧没有；构造一个 old 可见位置 seq 4 的在途：
    # 通过先声明空号区间让游标越过 3 号缺口。
    old_producer.complete_epoch("botA", 1, last_seq=4, gaps=[[3, 3]])
    # 已收内容不能报空。
    try:
        old_producer.complete_epoch("botA", 1, last_seq=4, gaps=[[2, 2]])
        raise AssertionError("已有内容的位置不能通过报空跳过")
    except ProtocolViolation:
        pass

    # 旧组现在能拿到 seq 2 吗？不能（内容只在 new）；先由新流补一份 old 通道副本，
    # 模拟旧流迟到的同内容事件。
    old_producer.emit("botA", 1, 2, "o-2b", {"step": "work"})
    poll = slow.poll("botA", "m1", "d2")
    expect(poll["result"] == "delivered" and poll["delivery"]["transport"] == "o-2b",
           f"旧组应从 old 通道拿到裁定内容: {poll}")

    # 工作进程丢失：协调器过期，组内其他成员先接回同一位置（此时尚未申请切换）。
    coordinator.expire("slow", "botA", "m1", "d2")
    # 旧进程再次出现不能替新成员确认。
    stale = slow.ack("botA", "m1", "d2")
    expect(stale["outcome"] == "rejected_stale", f"过期后旧确认必须拒绝: {stale}")

    takeover = slow.poll("botA", "m2", "d3")
    expect(takeover["result"] == "redelivered" and takeover["delivery"]["member"] == "m2"
           and takeover["delivery"]["channel"] == OLD,
           f"组内成员应在 old 上接回未确认位置: {takeover}")
    not_holder = slow.ack("botA", "m1", "d3")
    expect(not_holder["outcome"] == "rejected_not_holder",
           f"非持有者不能确认: {not_holder}")

    # 此刻仍持有可完成的旧交付，切换申请必须等待。
    switch = slow.switch("botA", 1, 2)
    expect(switch["outcome"] == "waiting_in_flight", f"有旧在途时应等待: {switch}")
    waiting = coordinator.inspect("botA", "slow")["group"]
    expect(waiting["classification"] == "old_delivery_completable"
           and waiting["switch_pending"],
           f"仍可完成的旧交付应被单独分类: {waiting['classification']}")
    slow.ack("botA", "m2", "d3")
    # 在途排空，下一次交互时切换自动完成。
    # seq 2 现已在 old 上确认，游标越过它；后续位置直接从新流接续。
    poll = slow.poll("botA", "m2", "d4")
    expect(poll["result"] == "delivered" and poll["delivery"]["channel"] == NEW
           and poll["delivery"]["seq"] == 4,
           f"切换后应从新流接续第一个未确认位置: {poll}")
    slow.ack("botA", "m2", "d4")

    # 3. 新组不等旧组，全程在 new 上按设备内顺序消费。 ------------------------------
    fresh_poll = fresh.poll("botA", "m9", "f1")
    expect(fresh_poll["delivery"]["seq"] == 1, "新组从最早位置开始")
    fresh.ack("botA", "m9", "f1")

    # 4. 代次：旧代次不收尾，新代次不能冲进来。 -------------------------------------
    new_producer.emit("botE", 1, 1, "e-n1", {"step": "load"})
    try:
        new_producer.emit("botE", 2, 1, "e-n2-1", {"step": "load"})
        raise AssertionError("旧代次未完结时新代次不得写入")
    except ProtocolViolation:
        pass
    # 旧代次完结后新代次正常写入，且从 1 开始编号也不会与旧代次位置混淆。
    new_producer.complete_epoch("botE", 1, last_seq=1, gaps=[])
    ok = new_producer.emit("botE", 2, 1, "e-n2-1", {"step": "load"})
    expect(ok["outcome"] == "added", f"新代次从头编号应被接纳: {ok}")

    # 5. 下线：条件不满足时阻塞并给出分类，满足后成功。 ------------------------------
    # 再加一个已申请切换但游标还没越过交接点的组。
    laggard = ConsumerGroupAdapter(bus, "laggard", clock)
    laggard.subscribe("botA", OLD)
    laggard.switch("botA", 1, 2)  # 无在途，立即落到新流，但游标仍在 (1,1)
    blocked_retire = coordinator.retire("botA", 1, 4)
    expect(blocked_retire["outcome"] == "blocked", "组未越过交接点时必须阻塞下线")
    kinds = {b["kind"] for b in blocked_retire["blockers"]}
    show("retire 阻塞证据", blocked_retire)
    expect("group_behind_handoff" in kinds,
           f"组进度未越过交接点时应阻塞: {kinds}")

    # fresh 是 new 通道订阅，不阻塞老通道下线；把 laggard 追到越过交接点。
    for delivery_id in ("g1", "g2", "g3", "g4"):
        reply = laggard.poll("botA", "m8", delivery_id)
        if reply["result"] == "delivered":
            laggard.ack("botA", "m8", delivery_id)
    # fresh 也追到末尾。
    for delivery_id in ("f2", "f3", "f4"):
        reply = fresh.poll("botA", "m9", delivery_id)
        if reply["result"] == "delivered":
            fresh.ack("botA", "m9", delivery_id)
    report = coordinator.inspect("botA", "slow")
    expect(report["group"]["classification"] == "catching_up",
           f"无冲突无缺口时应归类为单纯进度慢: {report['group']['classification']}")
    retired = coordinator.retire("botA", 1, 4)
    expect(retired["outcome"] == "retired", f"条件齐备后应允许下线: {retired}")
    try:
        old_producer.emit("botA", 1, 5, "o-5", {"step": "late"})
        raise AssertionError("下线后 old 通道必须拒绝新记录")
    except ProtocolViolation:
        pass

    # 6. 第二台设备：退订路径也能解除下线阻塞。 --------------------------------------
    slow.subscribe("botB", OLD)
    old_producer.emit("botB", 1, 1, "b-o1", {"step": "load"})
    old_producer.complete_epoch("botB", 1, last_seq=1, gaps=[])
    blocked_b = coordinator.retire("botB", 1, 1)
    expect(any(b.get("group") == "slow" for b in blocked_b["blockers"]),
           "仍在 old 的组应阻塞 botB 下线")
    slow.unsubscribe("botB")
    retired_b = coordinator.retire("botB", 1, 1)
    expect(retired_b["outcome"] == "retired", f"退订后应可下线: {retired_b}")

    # 7. 查询分类：暂缺事件 vs 冲突 vs 旧在途。 --------------------------------------
    old_producer.emit("botC", 1, 1, "c-o1", {"step": "load"})
    old_producer.emit("botC", 1, 3, "c-o3", {"step": "done"})
    old_producer.complete_epoch("botC", 1, last_seq=3, gaps=[])
    g = ConsumerGroupAdapter(bus, "g", clock)
    g.subscribe("botC", OLD)
    g.poll("botC", "w", "c1")
    g.ack("botC", "w", "c1")
    waiting = g.poll("botC", "w", "c2")
    expect(waiting["result"] == "gap_pending", f"缺号应表现为 gap_pending: {waiting}")
    expect(bus.inspect("botC", "g")["group"]["classification"] == "event_missing",
           "暂缺事件应被单独分类")

    # 8. 重启恢复：未确认交付不能变成已处理。 ----------------------------------------
    with tempfile.TemporaryDirectory() as directory:
        store = EventStore(directory)
        live = MigrationBus()
        from replay import dispatch

        events = [
            {"timestamp": 0, "type": "subscribe",
             "data": {"group": "rx", "device": "botD", "channel": "old"}},
            {"timestamp": 1, "type": "event",
             "data": {"device": "botD", "epoch": 1, "seq": 1, "channel": "old",
                      "transport": "d1", "body": {"step": "load"}}},
            {"timestamp": 2, "type": "poll",
             "data": {"group": "rx", "device": "botD", "member": "p1", "delivery": "x1"}},
        ]
        applied = 0
        for event in events:
            store.append(event)
            dispatch(live, event)
            applied += 1
        store.save_snapshot(live, applied)
        snapshot = store.load_snapshot()
        reborn = MigrationBus.restore(snapshot["state"])
        group_report = reborn.inspect("botD", "rx")["group"]
        expect(len(group_report["in_flight"]) == 1
               and group_report["in_flight"][0]["status"] == HELD,
               "重启后未确认交付必须仍是在途，不得算作已处理")
        # 重启后由另一个成员接回并确认。
        reborn.expire("rx", "botD", "p1", "x1", 99)
        reborn.poll("rx", "botD", "p2", "x2", 100)
        ack = reborn.ack("rx", "botD", "p2", "x2", 101)
        expect(ack["outcome"] == "acked", "重启接回后应能正常确认")
        store.close()

    show("最终设备总览（botA）", coordinator.inspect("botA"))
    print("\n所有演练断言通过。")


if __name__ == "__main__":
    main()
