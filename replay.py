"""把事件契约（trace JSONL）回放到 MigrationBus，并提供本地 CLI。

用法：
  python3 replay.py run data/handoff-with-unacked-work.jsonl
  python3 replay.py run data/*.jsonl --store-dir .state/<场景>
  python3 replay.py restart-demo data/handoff-with-unacked-work.jsonl --store-dir .state/restart

回放按文件中的逻辑秒点顺序处理；每个 inspect 事件会把查询结果打印出来。
--strict 时任何 ProtocolViolation 都会中断，默认只记录被拒绝的请求。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Optional

from event_store import EventStore
from migration_bus import MigrationBus, ProtocolViolation
from trace_io import read_trace

ALLOWED_TYPES = {
    "ack",
    "complete_epoch",
    "event",
    "expire",
    "giveback",
    "inspect",
    "poll",
    "resolve",
    "retire",
    "subscribe",
    "switch",
    "unsubscribe",
}


def dispatch(bus: MigrationBus, event: dict[str, Any]) -> dict[str, Any]:
    """把单个契约事件映射到总线方法，返回总线结果。"""
    data = event["data"]
    kind = event["type"]
    ts = event["timestamp"]

    if kind == "event":
        return bus.ingest(
            device=data["device"],
            epoch=data["epoch"],
            seq=data["seq"],
            channel=data["channel"],
            transport=data["transport"],
            body=data["body"],
            timestamp=ts,
        )
    if kind == "complete_epoch":
        return bus.declare_complete(
            device=data["device"],
            epoch=data["epoch"],
            last_seq=data["last_seq"],
            gaps=data.get("gaps", []),
            timestamp=ts,
        )
    if kind == "resolve":
        return bus.resolve(
            data["device"], data["epoch"], data["seq"], data["transport"], ts
        )
    if kind == "subscribe":
        return bus.subscribe(data["group"], data["device"], data["channel"], ts)
    if kind == "unsubscribe":
        return bus.unsubscribe(data["group"], data["device"], ts)
    if kind == "poll":
        return bus.poll(
            data["group"], data["device"], data["member"], data["delivery"], ts
        )
    if kind == "ack":
        return bus.ack(
            data["group"], data["device"], data["member"], data["delivery"], ts
        )
    if kind == "giveback":
        return bus.giveback(
            data["group"], data["device"], data["member"], data["delivery"], ts
        )
    if kind == "expire":
        return bus.expire(
            data["group"], data["device"], data["member"], data["delivery"], ts
        )
    if kind == "switch":
        through = data["through"]
        return bus.request_switch(
            data["group"], data["device"], through["epoch"], through["seq"], ts
        )
    if kind == "retire":
        through = data["through"]
        return bus.retire(data["device"], through["epoch"], through["seq"], ts)
    if kind == "inspect":
        return bus.inspect(data["device"], data.get("group"))
    raise ValueError(f"未知事件类型 {kind!r}")


def replay_path(
    path: Path,
    bus: MigrationBus,
    store: Optional["EventStoreHandle"] = None,
    strict: bool = False,
    verbose: bool = True,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for event in read_trace(path, ALLOWED_TYPES):
        if store is not None:
            store.append(event)
        try:
            result = dispatch(bus, event)
        except ProtocolViolation as error:
            result = {
                "type": event["type"],
                "outcome": "violation",
                "error": str(error),
                "timestamp": event["timestamp"],
            }
            if strict:
                raise
        results.append(result)
        if verbose and (event["type"] == "inspect" or result.get("outcome") in (
            "violation",
            "rejected_not_holder",
            "rejected_stale",
            "blocked",
        )):
            print(json.dumps(result, ensure_ascii=False, indent=2))
    if store is not None:
        store.checkpoint(bus)
    return results


class EventStoreHandle:
    """EventStore 的运行时包装，维护已应用事件计数。"""

    def __init__(self, store: EventStore) -> None:
        self.store = store
        snapshot = store.load_snapshot()
        if snapshot is None:
            self.bus = MigrationBus()
            self.applied = 0
        else:
            self.bus = MigrationBus.restore(snapshot["state"])
            self.applied = snapshot["applied_events"]
        # 快照之后可能还有已 fsync 但未入快照的事件，重放尾部补齐。
        journal = store.read_journal()
        for event in journal[self.applied:]:
            try:
                dispatch(self.bus, event)
            except ProtocolViolation:
                # 与主回放路径一致：违规请求不破坏已持久化状态。
                pass
            self.applied += 1

    def append(self, event: dict[str, Any]) -> None:
        self.store.append(event)
        self.applied += 1

    def checkpoint(self, bus: MigrationBus) -> None:
        self.store.save_snapshot(bus, self.applied)

    def close(self) -> None:
        self.store.close()


def _load_or_create(store_dir: Optional[Path]) -> tuple[MigrationBus, Optional[EventStoreHandle]]:
    if store_dir is None:
        return MigrationBus(), None
    handle = EventStoreHandle(EventStore(store_dir))
    return handle.bus, handle


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="本地回放产线通道迁移演练")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="回放一个或多个 trace 文件")
    run_p.add_argument("paths", type=Path, nargs="+")
    run_p.add_argument("--store-dir", type=Path, default=None)
    run_p.add_argument("--strict", action="store_true")

    restart_p = sub.add_parser(
        "restart-demo", help="在中途重启进程，验证未确认交付不丢失"
    )
    restart_p.add_argument("path", type=Path)
    restart_p.add_argument("--store-dir", type=Path, required=True)

    args = parser.parse_args(argv)

    if args.command == "run":
        bus, store = _load_or_create(args.store_dir)
        try:
            for path in args.paths:
                print(f"== 回放 {path} ==")
                replay_path(path, bus, store, strict=args.strict)
        finally:
            if store is not None:
                store.close()
        return 0

    # restart-demo：分两段处理同一 trace，中间重建总线实例。
    events = list(read_trace(args.path, ALLOWED_TYPES))
    cut = max(1, len(events) // 2)
    store = EventStore(args.store_dir)
    try:
        bus = MigrationBus()
        for event in events[:cut]:
            store.append(event)
            dispatch(bus, event)
        store.save_snapshot(bus, cut)
        before = bus.snapshot()

        snapshot = store.load_snapshot()
        bus2 = MigrationBus.restore(snapshot["state"])
        applied = snapshot["applied_events"]
        for event in events[cut:]:
            store.append(event)
            dispatch(bus2, event)
            applied += 1
        store.save_snapshot(bus2, applied)
        after = bus2.snapshot()

        # 对照：一次性跑完的结果必须与分段重启一致。
        reference = MigrationBus()
        for event in events:
            dispatch(reference, event)
        expected = reference.snapshot()
        ok = json.dumps(after, sort_keys=True, ensure_ascii=False) == json.dumps(
            expected, sort_keys=True, ensure_ascii=False
        )
        print(json.dumps(
            {
                "type": "restart_demo",
                "events": len(events),
                "cut_at": cut,
                "restart_recovered_delivery_groups": len(before["deliveries"]),
                "matches_single_run": ok,
            },
            ensure_ascii=False,
            indent=2,
        ))
        return 0 if ok else 1
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
