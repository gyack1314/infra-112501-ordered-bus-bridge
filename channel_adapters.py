"""old/new 通道的本地适配方。

真实总线各有 SDK；本地演练用同一组适配方把生产方、消费组和协调器角色
接到 MigrationBus 上。new 适配方与 old 适配方走完全相同的业务规则，
差别只在事件标注的 channel，方便验证双写、切换和分批下线。
"""

from __future__ import annotations

from typing import Any, Optional

from migration_bus import MigrationBus, NEW, OLD


class ProducerAdapter:
    """生产侧角色：发事件、宣告代次完结与空号、裁定冲突。"""

    def __init__(self, bus: MigrationBus, channel: str, clock: "LogicalClock") -> None:
        if channel not in (OLD, NEW):
            raise ValueError(f"未知通道 {channel!r}")
        self.bus = bus
        self.channel = channel
        self.clock = clock

    def emit(
        self, device: str, epoch: int, seq: int, transport: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        return self.bus.ingest(
            device=device,
            epoch=epoch,
            seq=seq,
            channel=self.channel,
            transport=transport,
            body=body,
            timestamp=self.clock.now(),
        )

    def complete_epoch(
        self, device: str, epoch: int, last_seq: int, gaps: Optional[list[list[int]]] = None
    ) -> dict[str, Any]:
        return self.bus.declare_complete(
            device, epoch, last_seq, gaps or [], self.clock.now()
        )

    def resolve(self, device: str, epoch: int, seq: int, transport: str) -> dict[str, Any]:
        return self.bus.resolve(device, epoch, seq, transport, self.clock.now())


class ConsumerGroupAdapter:
    """消费组角色：订阅、拉取、确认/退回、切换、退订。"""

    def __init__(self, bus: MigrationBus, group: str, clock: "LogicalClock") -> None:
        self.bus = bus
        self.group = group
        self.clock = clock

    def subscribe(self, device: str, channel: str) -> dict[str, Any]:
        return self.bus.subscribe(self.group, device, channel, self.clock.now())

    def unsubscribe(self, device: str) -> dict[str, Any]:
        return self.bus.unsubscribe(self.group, device, self.clock.now())

    def poll(self, device: str, member: str, delivery_id: str) -> dict[str, Any]:
        return self.bus.poll(self.group, device, member, delivery_id, self.clock.now())

    def ack(self, device: str, member: str, delivery_id: str) -> dict[str, Any]:
        return self.bus.ack(self.group, device, member, delivery_id, self.clock.now())

    def giveback(self, device: str, member: str, delivery_id: str) -> dict[str, Any]:
        return self.bus.giveback(self.group, device, member, delivery_id, self.clock.now())

    def switch(self, device: str, through_epoch: int, through_seq: int) -> dict[str, Any]:
        return self.bus.request_switch(
            self.group, device, through_epoch, through_seq, self.clock.now()
        )


class CoordinatorAdapter:
    """协调器/运营角色：持有权超时、下线、查询。"""

    def __init__(self, bus: MigrationBus, clock: "LogicalClock") -> None:
        self.bus = bus
        self.clock = clock

    def expire(self, group: str, device: str, member: str, delivery_id: str) -> dict[str, Any]:
        return self.bus.expire(group, device, member, delivery_id, self.clock.now())

    def retire(self, device: str, through_epoch: int, through_seq: int) -> dict[str, Any]:
        return self.bus.retire(device, through_epoch, through_seq, self.clock.now())

    def inspect(self, device: str, group: Optional[str] = None) -> dict[str, Any]:
        return self.bus.inspect(device, group)


class LogicalClock:
    """与演练文件一致的逻辑秒点，单调递增。"""

    def __init__(self, start: int = 0) -> None:
        self._tick = start

    def now(self) -> int:
        value = self._tick
        self._tick += 1
        return value
