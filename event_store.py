"""中间件的本地持久化：事件日志 + 状态快照。

重启语义靠两点保证：
1. 每条输入事件按到达顺序追加写入 JSONL 日志（fsync 后返回）；
2. 重启时先载入最近快照，再重放快照之后的日志，未确认交付随快照/重放完整恢复，
   绝不会把某组未确认的工作视作已处理。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from migration_bus import MigrationBus


class EventStore:
    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.journal_path = self.directory / "journal.jsonl"
        self.snapshot_path = self.directory / "snapshot.json"
        self._journal = self.journal_path.open("a", encoding="utf-8")

    def append(self, event: dict[str, Any]) -> None:
        self._journal.write(json.dumps(event, ensure_ascii=False) + "\n")
        self._journal.flush()
        os.fsync(self._journal.fileno())

    def read_journal(self) -> list[dict[str, Any]]:
        if not self.journal_path.exists():
            return []
        with self.journal_path.open(encoding="utf-8") as source:
            return [json.loads(line) for line in source if line.strip()]

    def save_snapshot(self, bus: MigrationBus, applied_events: int) -> None:
        payload = {"applied_events": applied_events, "state": bus.snapshot()}
        tmp = self.snapshot_path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp, self.snapshot_path)

    def load_snapshot(self) -> Optional[dict[str, Any]]:
        if not self.snapshot_path.exists():
            return None
        return json.loads(self.snapshot_path.read_text(encoding="utf-8"))

    def close(self) -> None:
        self._journal.close()

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
