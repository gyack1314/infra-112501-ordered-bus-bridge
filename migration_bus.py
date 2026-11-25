"""跨流去重与消费接续的核心状态机。

业务位置由 (device, epoch, seq) 唯一确定；old/new 两条通道只决定传输来源，
不影响业务位置的身份。所有规则（双写去重、冲突保留、缺口/空号、代次顺序、
组切换、持有权交接、下线、查询分类）都集中在本模块，且不依赖任何真实总线，
可用合成角色在本地完整演练。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import threading
from typing import Any, Optional


OLD = "old"
NEW = "new"
CHANNELS = (OLD, NEW)

# 交付状态
HELD = "held"          # 仍可完成的在途交付
EXPIRED = "expired"    # 持有权超时，等待组内重新领回
GIVEBACK = "giveback"  # 持有者主动退回
CLOSED = "closed"      # 切换时被关闭的旧在途，必须从新侧重投
ACKED = "acked"        # 已确认


class ProtocolViolation(Exception):
    """生产方/消费方的请求与事件契约冲突（如对已收内容报空、越权 ack）。"""


@dataclass
class Record:
    """同一业务位置上的一份传输副本。"""

    transport: str
    channel: str
    body: dict[str, Any]
    accepted_at: int  # 接纳时的逻辑秒点

    def to_dict(self) -> dict[str, Any]:
        return {
            "transport": self.transport,
            "channel": self.channel,
            "body": self.body,
            "accepted_at": self.accepted_at,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Record":
        return cls(
            transport=raw["transport"],
            channel=raw["channel"],
            body=raw["body"],
            accepted_at=raw["accepted_at"],
        )


@dataclass
class Position:
    """单个业务位置的接纳/冲突/交付状态。"""

    epoch: int
    seq: int
    records: dict[str, Record] = field(default_factory=dict)  # transport -> record
    body_index: dict[str, list[str]] = field(default_factory=dict)  # 规范体 -> transports
    conflict: bool = False
    resolved_transport: Optional[str] = None
    delivered: bool = False
    delivered_transport: Optional[str] = None
    post_delivery_conflicts: list[str] = field(default_factory=list)

    @property
    def has_content(self) -> bool:
        return bool(self.records)

    @property
    def canonical_transport(self) -> Optional[str]:
        if self.resolved_transport is not None:
            return self.resolved_transport
        if not self.records:
            return None
        # 无冲突时所有副本内容相同，取最早接纳的一份。
        return min(self.records, key=lambda t: self.records[t].accepted_at)

    def canonical_body(self) -> Optional[dict[str, Any]]:
        transport = self.canonical_transport
        if transport is None:
            return None
        return self.records[transport].body

    def add(self, record: Record) -> str:
        """接纳一份新副本，返回 deduped / conflicted / added。"""
        prior = self.records.get(record.transport)
        if prior is not None:
            if prior.body == record.body:
                return "deduped"  # 同一传输标识的重试且内容相同
            raise ProtocolViolation(
                f"transport {record.transport!r} 在同一业务位置送来不同内容"
            )
        body_key = _body_key(record.body)
        same_body = self.body_index.get(body_key, [])
        self.records[record.transport] = record
        self.body_index.setdefault(body_key, []).append(record.transport)
        if same_body:
            return "deduped"  # 不同传输标识但内容相同（双写），只算一次业务事件
        if self.delivered:
            # 位置一经交付，后续冲突只留证，不改变已交付内容。
            self.post_delivery_conflicts.append(record.transport)
            return "conflicted"
        # 同一业务位置出现了第二份不同内容，且尚未交付：卡住等待裁定。
        if len(self.body_index) > 1:
            self.conflict = True
            self.resolved_transport = None
            return "conflicted"
        return "added"

    def resolve(self, transport: str) -> str:
        if transport not in self.records:
            raise ProtocolViolation("resolve 引用的 transport 未在该位置出现过")
        if self.delivered:
            raise ProtocolViolation("该位置已经交付，不能再改变裁定")
        if not self.conflict:
            if self.resolved_transport == transport:
                return "unchanged"
            raise ProtocolViolation("该位置不存在待裁定冲突")
        self.resolved_transport = transport
        self.conflict = False
        return "resolved"

    def state(self) -> str:
        if self.conflict:
            return "conflict"
        if not self.records:
            return "empty"
        if self.delivered:
            return "delivered"
        return "ready"

    def to_dict(self) -> dict[str, Any]:
        return {
            "epoch": self.epoch,
            "seq": self.seq,
            "records": [r.to_dict() for r in self.records.values()],
            "conflict": self.conflict,
            "resolved_transport": self.resolved_transport,
            "delivered": self.delivered,
            "delivered_transport": self.delivered_transport,
            "post_delivery_conflicts": list(self.post_delivery_conflicts),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Position":
        position = cls(epoch=raw["epoch"], seq=raw["seq"])
        for item in raw["records"]:
            record = Record.from_dict(item)
            position.records[record.transport] = record
            position.body_index.setdefault(_body_key(record.body), []).append(record.transport)
        position.conflict = raw["conflict"]
        position.resolved_transport = raw["resolved_transport"]
        position.delivered = raw["delivered"]
        position.delivered_transport = raw["delivered_transport"]
        position.post_delivery_conflicts = list(raw.get("post_delivery_conflicts", []))
        return position


@dataclass
class Epoch:
    epoch: int
    positions: dict[int, Position] = field(default_factory=dict)
    last_seq: Optional[int] = None
    gaps: list[tuple[int, int]] = field(default_factory=list)

    def position(self, seq: int) -> Position:
        if seq not in self.positions:
            self.positions[seq] = Position(epoch=self.epoch, seq=seq)
        return self.positions[seq]

    def declared_gap(self, seq: int) -> bool:
        """生产方是否明确声明该号为空号区间。"""
        if self.last_seq is None or not (1 <= seq <= self.last_seq):
            return False
        return any(lo <= seq <= hi for lo, hi in self.gaps)

    def is_empty(self, seq: int) -> bool:
        """已声明范围内既无内容、又被显式报空的位置。"""
        if not self.declared_gap(seq):
            return False
        position = self.positions.get(seq)
        return position is None or not position.has_content

    def to_dict(self) -> dict[str, Any]:
        return {
            "epoch": self.epoch,
            "positions": [p.to_dict() for p in self.positions.values()],
            "last_seq": self.last_seq,
            "gaps": [list(gap) for gap in self.gaps],
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Epoch":
        epoch = cls(epoch=raw["epoch"], last_seq=raw["last_seq"])
        epoch.gaps = [tuple(gap) for gap in raw.get("gaps", [])]  # type: ignore[misc]
        for item in raw["positions"]:
            position = Position.from_dict(item)
            epoch.positions[position.seq] = position
        return epoch


@dataclass
class Delivery:
    """一次组内交付（delivery 标识由 poll 请求给出）。"""

    delivery_id: str
    epoch: int
    seq: int
    member: str
    channel: str
    transport: Optional[str]
    status: str = HELD
    created_at: int = 0
    closed_at: Optional[int] = None

    def key(self) -> tuple[int, int]:
        return self.epoch, self.seq

    def to_dict(self) -> dict[str, Any]:
        return {
            "delivery_id": self.delivery_id,
            "epoch": self.epoch,
            "seq": self.seq,
            "member": self.member,
            "channel": self.channel,
            "transport": self.transport,
            "status": self.status,
            "created_at": self.created_at,
            "closed_at": self.closed_at,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Delivery":
        return cls(**raw)


@dataclass
class Subscription:
    group: str
    device: str
    channel: str = OLD
    cursor_epoch: Optional[int] = None   # 下一个待处理位置
    cursor_seq: Optional[int] = None
    switch_pending: bool = False
    switch_through: Optional[tuple[int, int]] = None
    switched_at: Optional[int] = None
    unsubscribed_at: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "group": self.group,
            "device": self.device,
            "channel": self.channel,
            "cursor_epoch": self.cursor_epoch,
            "cursor_seq": self.cursor_seq,
            "switch_pending": self.switch_pending,
            "switch_through": list(self.switch_through) if self.switch_through else None,
            "switched_at": self.switched_at,
            "unsubscribed_at": self.unsubscribed_at,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Subscription":
        sub = cls(
            group=raw["group"],
            device=raw["device"],
            channel=raw["channel"],
            cursor_epoch=raw["cursor_epoch"],
            cursor_seq=raw["cursor_seq"],
            switch_pending=raw["switch_pending"],
            switched_at=raw.get("switched_at"),
            unsubscribed_at=raw.get("unsubscribed_at"),
        )
        if raw.get("switch_through"):
            sub.switch_through = tuple(raw["switch_through"])  # type: ignore[assignment]
        return sub


@dataclass
class DeviceState:
    device: str
    epochs: dict[int, Epoch] = field(default_factory=dict)
    retired_at: Optional[int] = None
    retire_through: Optional[tuple[int, int]] = None

    def epoch(self, epoch: int) -> Epoch:
        if epoch not in self.epochs:
            self.epochs[epoch] = Epoch(epoch=epoch)
        return self.epochs[epoch]

    def get_position(self, epoch: int, seq: int) -> Optional[Position]:
        current = self.epochs.get(epoch)
        if current is None:
            return None
        return current.positions.get(seq)

    def to_dict(self) -> dict[str, Any]:
        return {
            "device": self.device,
            "epochs": [e.to_dict() for e in self.epochs.values()],
            "retired_at": self.retired_at,
            "retire_through": list(self.retire_through) if self.retire_through else None,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "DeviceState":
        state = cls(device=raw["device"], retired_at=raw.get("retired_at"))
        if raw.get("retire_through"):
            state.retire_through = tuple(raw["retire_through"])  # type: ignore[assignment]
        for item in raw["epochs"]:
            epoch = Epoch.from_dict(item)
            state.epochs[epoch.epoch] = epoch
        return state


def _body_key(body: dict[str, Any]) -> str:
    """载荷按字段值判同：与键顺序、空白无关的规范 JSON 串。"""
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class MigrationBus:
    """中间件本体。方法入参对应事件契约，返回值为可 JSON 化的结果字典。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.devices: dict[str, DeviceState] = {}
        self.subscriptions: dict[tuple[str, str], Subscription] = {}
        self.deliveries: dict[tuple[str, str], dict[str, Delivery]] = {}

    # ------------------------------------------------------------------ 工具

    def _device(self, device: str) -> DeviceState:
        if device not in self.devices:
            self.devices[device] = DeviceState(device=device)
        return self.devices[device]

    def _sub(self, group: str, device: str) -> Subscription:
        sub = self.subscriptions.get((group, device))
        if sub is None:
            raise ProtocolViolation(f"组 {group!r} 未订阅设备 {device!r}")
        if sub.unsubscribed_at is not None:
            raise ProtocolViolation(f"组 {group!r} 已退订设备 {device!r}")
        return sub

    def _group_deliveries(self, group: str, device: str) -> dict[str, Delivery]:
        return self.deliveries.setdefault((group, device), {})

    # ------------------------------------------------------------ 生产侧入口

    def ingest(
        self,
        device: str,
        epoch: int,
        seq: int,
        channel: str,
        transport: str,
        body: dict[str, Any],
        timestamp: int,
    ) -> dict[str, Any]:
        with self._lock:
            if channel not in CHANNELS:
                raise ProtocolViolation(f"未知通道 {channel!r}")
            if not isinstance(seq, int) or seq < 1:
                raise ProtocolViolation("seq 必须是从 1 起的正整数")
            device_state = self._device(device)
            if device_state.retired_at is not None and channel == OLD:
                raise ProtocolViolation(
                    f"设备 {device!r} 的 old 通道已下线，不再接纳旧流记录"
                )
            # 新代次不能在旧代次未完结时冲进来。
            for old_epoch in device_state.epochs:
                if old_epoch < epoch and device_state.epochs[old_epoch].last_seq is None:
                    raise ProtocolViolation(
                        f"代次 {old_epoch} 尚未宣告完结，代次 {epoch} 不得提前写入"
                    )
            epoch_state = device_state.epoch(epoch)
            if epoch_state.last_seq is not None and seq > epoch_state.last_seq:
                raise ProtocolViolation(
                    f"代次 {epoch} 已宣告末号 {epoch_state.last_seq}，seq={seq} 越界"
                )
            if epoch_state.declared_gap(seq):
                raise ProtocolViolation(
                    f"位置 (epoch={epoch}, seq={seq}) 已被生产方声明为空号，不能再写入"
                )
            # 订阅早于首条事件的组，在真实代次首次出现时惰性对齐游标。
            for sub in self._subs_of(device):
                self._ensure_cursor(sub, device_state)
            position = epoch_state.position(seq)
            record = Record(
                transport=transport,
                channel=channel,
                body=body,
                accepted_at=timestamp,
            )
            outcome = position.add(record)
            return {
                "type": "ingest",
                "device": device,
                "epoch": epoch,
                "seq": seq,
                "transport": transport,
                "channel": channel,
                "outcome": outcome,
                "position_state": position.state(),
            }

    def declare_complete(
        self,
        device: str,
        epoch: int,
        last_seq: int,
        gaps: list[tuple[int, int] | list[int]],
        timestamp: int,
    ) -> dict[str, Any]:
        with self._lock:
            if last_seq < 0:
                raise ProtocolViolation("last_seq 不能为负")
            normalized: list[tuple[int, int]] = []
            for gap in gaps:
                lo, hi = int(gap[0]), int(gap[1])
                if lo < 1 or hi < lo or hi > last_seq:
                    raise ProtocolViolation(f"非法空号区间 {list(gap)!r}")
                normalized.append((lo, hi))
            device_state = self._device(device)
            epoch_state = device_state.epoch(epoch)
            # 已接收了内容的位置不能通过报空跳过。
            for lo, hi in normalized:
                for seq in range(lo, hi + 1):
                    position = epoch_state.positions.get(seq)
                    if position is not None and position.has_content:
                        raise ProtocolViolation(
                            f"位置 (epoch={epoch}, seq={seq}) 已有内容，不能报空"
                        )
            if epoch_state.last_seq is not None:
                if epoch_state.last_seq != last_seq or sorted(epoch_state.gaps) != sorted(normalized):
                    raise ProtocolViolation("同一代次的完结声明必须一致")
                return {
                    "type": "complete_epoch",
                    "device": device,
                    "epoch": epoch,
                    "outcome": "unchanged",
                }
            for seq, position in epoch_state.positions.items():
                if seq > last_seq:
                    raise ProtocolViolation(
                        f"已接收 seq={seq}，超出声明末号 {last_seq}"
                    )
            epoch_state.last_seq = last_seq
            epoch_state.gaps = normalized
            for sub in self._subs_of(device):
                self._ensure_cursor(sub, device_state)
                self._advance_cursor(sub, device_state)
            return {
                "type": "complete_epoch",
                "device": device,
                "epoch": epoch,
                "last_seq": last_seq,
                "gaps": [list(gap) for gap in normalized],
                "outcome": "declared",
            }

    def resolve(
        self, device: str, epoch: int, seq: int, transport: str, timestamp: int
    ) -> dict[str, Any]:
        with self._lock:
            device_state = self._device(device)
            position = device_state.get_position(epoch, seq)
            if position is None:
                raise ProtocolViolation("resolve 的位置没有任何接收记录")
            outcome = position.resolve(transport)
            return {
                "type": "resolve",
                "device": device,
                "epoch": epoch,
                "seq": seq,
                "transport": transport,
                "outcome": outcome,
                "position_state": position.state(),
            }

    # ------------------------------------------------------------ 组生命周期

    def subscribe(self, group: str, device: str, channel: str, timestamp: int) -> dict[str, Any]:
        with self._lock:
            if channel not in CHANNELS:
                raise ProtocolViolation(f"未知通道 {channel!r}")
            key = (group, device)
            existing = self.subscriptions.get(key)
            if existing is not None and existing.unsubscribed_at is None:
                if existing.channel != channel:
                    raise ProtocolViolation(
                        "通道变更必须通过 switch，不能重复 subscribe 到另一通道"
                    )
                return {"type": "subscribe", "group": group, "device": device, "outcome": "unchanged"}
            sub = Subscription(group=group, device=device, channel=channel)
            self.subscriptions[key] = sub
            device_state = self._device(device)
            self._init_cursor(sub, device_state)
            return {
                "type": "subscribe",
                "group": group,
                "device": device,
                "channel": channel,
                "outcome": "subscribed",
            }

    def unsubscribe(self, group: str, device: str, timestamp: int) -> dict[str, Any]:
        with self._lock:
            sub = self._sub(group, device)
            sub.unsubscribed_at = timestamp
            # 退订后该组在该设备上的在途不再阻塞下线。
            for delivery in self._group_deliveries(group, device).values():
                if delivery.status == HELD:
                    delivery.status = CLOSED
                    delivery.closed_at = timestamp
            return {
                "type": "unsubscribe",
                "group": group,
                "device": device,
                "outcome": "unsubscribed",
            }

    def _subs_of(self, device: str) -> list[Subscription]:
        return [
            sub
            for (group, dev), sub in self.subscriptions.items()
            if dev == device and sub.unsubscribed_at is None
        ]

    def _init_cursor(self, sub: Subscription, device_state: DeviceState) -> None:
        """新组没有已确认进度；设备尚无代次时惰性等待首条事件。"""
        if device_state.epochs:
            sub.cursor_epoch = min(device_state.epochs)
            sub.cursor_seq = 1
            self._advance_cursor(sub, device_state)
        else:
            sub.cursor_epoch = None
            sub.cursor_seq = None

    def _ensure_cursor(self, sub: Subscription, device_state: DeviceState) -> None:
        if sub.cursor_epoch is None and device_state.epochs:
            sub.cursor_epoch = min(device_state.epochs)
            sub.cursor_seq = 1
            self._advance_cursor(sub, device_state)

    def _advance_cursor(self, sub: Subscription, device_state: DeviceState) -> None:
        """只跳过生产方明确声明的空号；内容未到的缺口必须挡住后续交付。"""
        while True:
            if sub.cursor_epoch is None or sub.cursor_seq is None:
                return  # 设备尚无任何代次，游标惰性等待
            epoch_state = device_state.epochs.get(sub.cursor_epoch)
            if epoch_state is None or epoch_state.last_seq is None:
                return
            seq = sub.cursor_seq
            if seq > epoch_state.last_seq:
                # 本代次已全部越过，进入下一代次（若已知）。
                next_epochs = sorted(e for e in device_state.epochs if e > sub.cursor_epoch)
                if not next_epochs:
                    return
                sub.cursor_epoch = next_epochs[0]
                sub.cursor_seq = 1
                continue
            if epoch_state.declared_gap(seq):
                sub.cursor_seq = seq + 1
                continue
            return

    # ------------------------------------------------------------ 投递与确认

    def poll(
        self,
        group: str,
        device: str,
        member: str,
        delivery_id: str,
        timestamp: int,
    ) -> dict[str, Any]:
        with self._lock:
            sub = self._sub(group, device)
            device_state = self._device(device)
            group_map = self._group_deliveries(group, device)
            if delivery_id in group_map:
                raise ProtocolViolation(f"delivery 标识 {delivery_id!r} 已被使用")
            self._maybe_finalize_switch(sub, device, device_state, timestamp)
            self._ensure_cursor(sub, device_state)
            self._advance_cursor(sub, device_state)

            held = [d for d in group_map.values() if d.status == HELD]
            if held:
                # 一个组在同一设备上同时只持有一个未完成交付，顺序不被并发打乱。
                return self._poll_result(delivery_id, None, reason="in_flight", held=held[0])

            # 丢了的交付（过期/退回/切换关闭）优先由组内其他成员接回同一位置。
            # 只接回当前通道的残留；切换前 old 通道的 closed 交付仅作历史证据，
            # 该位置由游标路径在新流上重新交付。
            reopen = next(
                (
                    d
                    for d in group_map.values()
                    if d.status in (EXPIRED, GIVEBACK, CLOSED) and d.channel == sub.channel
                ),
                None,
            )
            if reopen is not None:
                return self._redeliver(reopen, member, delivery_id, timestamp, group_map, device)

            if sub.cursor_epoch is None:
                return self._poll_result(
                    delivery_id, None, reason="gap_pending",
                    waiting={"epoch": None, "seq": None},
                )
            position = device_state.get_position(sub.cursor_epoch, sub.cursor_seq)
            if position is None:
                return self._poll_result(
                    delivery_id, None, reason="gap_pending",
                    waiting={"epoch": sub.cursor_epoch, "seq": sub.cursor_seq},
                )
            if position.conflict:
                return self._poll_result(
                    delivery_id, None, reason="conflict",
                    waiting={"epoch": position.epoch, "seq": position.seq},
                )
            record = self._record_for(sub, position)
            if record is None:
                # 内容只在组当前不可见的通道上（例如旧组而记录仅到了新流）。
                return self._poll_result(
                    delivery_id, None, reason="awaiting_channel",
                    waiting={"epoch": position.epoch, "seq": position.seq},
                )

            delivery = Delivery(
                delivery_id=delivery_id,
                epoch=position.epoch,
                seq=position.seq,
                member=member,
                channel=sub.channel,
                transport=record.transport,
                status=HELD,
                created_at=timestamp,
            )
            group_map[delivery_id] = delivery
            position.delivered = True
            position.delivered_transport = record.transport
            return self._poll_result(delivery_id, delivery)

    def _record_for(self, sub: Subscription, position: Position) -> Optional[Record]:
        canonical_body = position.canonical_body()
        if canonical_body is None:
            return None
        key = _body_key(canonical_body)
        candidates = [
            position.records[t] for t in position.body_index.get(key, [])
        ]
        # 已裁定/同内容的副本中，优先选组当前通道的传输标识。
        for record in candidates:
            if record.channel == sub.channel:
                return record
        # 通道不同：新侧组可直接读旧侧已接纳内容（交接后新流有权读取）；
        # 旧侧组不读取新侧副本，必须先完成切换。
        if sub.channel == NEW:
            transport = position.canonical_transport
            return position.records[transport] if transport else None
        return None

    def _redeliver(
        self,
        previous: Delivery,
        member: str,
        delivery_id: str,
        timestamp: int,
        group_map: dict[str, Delivery],
        device: str,
    ) -> dict[str, Any]:
        """组内成员接回未确认位置；旧 delivery 标识作废，旧持有者不能再确认。"""
        device_state = self._device(device)
        owner = self._owner_key(group_map)
        position = device_state.get_position(previous.epoch, previous.seq)
        record = self._record_for(self.subscriptions[owner], position) \
            if position is not None else None
        transport = record.transport if record is not None else previous.transport
        delivery = Delivery(
            delivery_id=delivery_id,
            epoch=previous.epoch,
            seq=previous.seq,
            member=member,
            channel=self.subscriptions[owner].channel,
            transport=transport,
            status=HELD,
            created_at=timestamp,
        )
        previous.status = CLOSED
        previous.closed_at = timestamp
        group_map[delivery_id] = delivery
        return self._poll_result(
            delivery_id,
            delivery,
            reason="redelivered",
        )

    def _owner_key(self, group_map: dict[str, Delivery]) -> tuple[str, str]:
        for key, mapping in self.deliveries.items():
            if mapping is group_map:
                return key
        raise RuntimeError("delivery 所属订阅丢失")

    def _poll_result(
        self,
        request_id: str,
        delivery: Optional[Delivery],
        reason: str = "delivered",
        held: Optional[Delivery] = None,
        waiting: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "type": "poll",
            "request": request_id,
            "result": reason,
        }
        if delivery is not None:
            result["delivery"] = delivery.to_dict()
        if held is not None:
            result["held_by"] = held.member
        if waiting is not None:
            result["waiting"] = waiting
        return result

    def _find_delivery(
        self, group: str, device: str, delivery_id: str
    ) -> Optional[Delivery]:
        return self._group_deliveries(group, device).get(delivery_id)

    def _settle(
        self,
        kind: str,
        group: str,
        device: str,
        member: str,
        delivery_id: str,
        timestamp: int,
    ) -> dict[str, Any]:
        with self._lock:
            sub = self._sub(group, device)
            delivery = self._find_delivery(group, device, delivery_id)
            if delivery is None:
                raise ProtocolViolation(f"未知 delivery {delivery_id!r}")
            if delivery.member != member:
                # 旧进程再次出现也不能替新持有者确认；ack/giveback 须来自持有者。
                return {
                    "type": kind,
                    "group": group,
                    "device": device,
                    "delivery": delivery_id,
                    "outcome": "rejected_not_holder",
                    "holder": delivery.member,
                }
            if kind == "ack":
                if delivery.status != HELD:
                    return {
                        "type": kind,
                        "group": group,
                        "device": device,
                        "delivery": delivery_id,
                        "outcome": "rejected_stale",
                        "status": delivery.status,
                    }
                delivery.status = ACKED
                delivery.closed_at = timestamp
                if (sub.cursor_epoch, sub.cursor_seq) == delivery.key():
                    sub.cursor_seq = delivery.seq + 1
                self._advance_cursor(sub, self._device(device))
                outcome = "acked"
            elif kind == "giveback":
                if delivery.status != HELD:
                    return {
                        "type": kind,
                        "group": group,
                        "device": device,
                        "delivery": delivery_id,
                        "outcome": "rejected_stale",
                        "status": delivery.status,
                    }
                delivery.status = GIVEBACK
                delivery.closed_at = timestamp
                outcome = "giveback"
            else:  # expire
                if delivery.status != HELD:
                    return {
                        "type": kind,
                        "group": group,
                        "device": device,
                        "delivery": delivery_id,
                        "outcome": "rejected_stale",
                        "status": delivery.status,
                    }
                delivery.status = EXPIRED
                delivery.closed_at = timestamp
                outcome = "expired"
            self._maybe_finalize_switch(sub, device, self._device(device), timestamp)
            return {
                "type": kind,
                "group": group,
                "device": device,
                "delivery": delivery_id,
                "outcome": outcome,
            }

    def ack(self, group: str, device: str, member: str, delivery_id: str, timestamp: int) -> dict[str, Any]:
        return self._settle("ack", group, device, member, delivery_id, timestamp)

    def giveback(self, group: str, device: str, member: str, delivery_id: str, timestamp: int) -> dict[str, Any]:
        return self._settle("giveback", group, device, member, delivery_id, timestamp)

    def expire(self, group: str, device: str, member: str, delivery_id: str, timestamp: int) -> dict[str, Any]:
        return self._settle("expire", group, device, member, delivery_id, timestamp)

    # ---------------------------------------------------------------- 切换

    def request_switch(
        self,
        group: str,
        device: str,
        through_epoch: int,
        through_seq: int,
        timestamp: int,
    ) -> dict[str, Any]:
        with self._lock:
            sub = self._sub(group, device)
            if sub.channel == NEW:
                raise ProtocolViolation("该组在此设备上已经位于新流")
            if sub.switch_pending:
                raise ProtocolViolation("切换请求已在等待旧在途排空")
            sub.switch_pending = True
            sub.switch_through = (through_epoch, through_seq)
            device_state = self._device(device)
            finalized = self._maybe_finalize_switch(sub, device, device_state, timestamp)
            return {
                "type": "switch",
                "group": group,
                "device": device,
                "through": {"epoch": through_epoch, "seq": through_seq},
                "outcome": "switched" if finalized else "waiting_in_flight",
            }

    def _completable_old_in_flight(self, sub: Subscription, group: str, device: str) -> list[Delivery]:
        """仍可完成的旧通道在途：仅 old 通道上的 held 交付。"""
        if sub.channel != OLD:
            return []
        return [
            d
            for d in self._group_deliveries(group, device).values()
            if d.status == HELD and d.channel == OLD
        ]

    def _maybe_finalize_switch(
        self, sub: Subscription, device: str, device_state: DeviceState, timestamp: int
    ) -> bool:
        if not sub.switch_pending or sub.channel == NEW:
            return False
        if self._completable_old_in_flight(sub, sub.group, device):
            return False
        # 没有仍可完成的旧在途：关闭残余旧交付（过期/退回的也不能再从旧侧重投），
        # 新流从组的第一个未确认位置接续，哪怕它位于原定交接点之前。
        for delivery in self._group_deliveries(sub.group, device).values():
            if delivery.status in (EXPIRED, GIVEBACK) and delivery.channel == OLD:
                delivery.status = CLOSED
                delivery.closed_at = timestamp
        sub.channel = NEW
        sub.switch_pending = False
        sub.switched_at = timestamp
        self._ensure_cursor(sub, device_state)
        self._advance_cursor(sub, device_state)
        return True

    # ---------------------------------------------------------------- 下线

    def retire(
        self, device: str, through_epoch: int, through_seq: int, timestamp: int
    ) -> dict[str, Any]:
        with self._lock:
            device_state = self._device(device)
            blockers: list[dict[str, Any]] = []

            if device_state.retired_at is not None:
                if device_state.retire_through == (through_epoch, through_seq):
                    return {
                        "type": "retire",
                        "device": device,
                        "outcome": "retired",
                        "through": {"epoch": through_epoch, "seq": through_seq},
                        "idempotent": True,
                    }
                raise ProtocolViolation(
                    "设备 old 通道已按另一范围下线，不能扩大下线范围"
                )

            epoch_state = device_state.epochs.get(through_epoch)
            if epoch_state is None or epoch_state.last_seq is None:
                blockers.append({
                    "kind": "producer_open",
                    "reason": f"代次 {through_epoch} 尚未宣告完结",
                })
            elif through_seq > epoch_state.last_seq:
                blockers.append({
                    "kind": "producer_open",
                    "reason": "through 超出已宣告末号",
                })
            else:
                # 旧流中已宣告完结的范围都必须被接纳（含冲突已裁定、空号已声明）。
                for epoch in sorted(device_state.epochs):
                    if epoch > through_epoch:
                        break
                    current = device_state.epochs[epoch]
                    upper = through_seq if epoch == through_epoch else (current.last_seq or 0)
                    if current.last_seq is None:
                        blockers.append({
                            "kind": "producer_open",
                            "epoch": epoch,
                            "reason": "代次尚未宣告完结",
                        })
                        continue
                    for seq in range(1, upper + 1):
                        if current.declared_gap(seq):
                            continue
                        position = current.positions.get(seq)
                        if position is None or not position.has_content:
                            blockers.append({
                                "kind": "missing_event",
                                "epoch": epoch,
                                "seq": seq,
                            })
                        elif position.conflict:
                            blockers.append({
                                "kind": "producer_conflict",
                                "epoch": epoch,
                                "seq": seq,
                            })

            for sub in self._subs_of(device):
                if sub.channel != OLD:
                    continue  # 新流订阅不依赖老通道
                entry: dict[str, Any] = {"kind": "group_old_channel", "group": sub.group}
                if sub.switch_pending:
                    entry["reason"] = "switch_waiting_in_flight"
                    entry["in_flight"] = [
                        d.to_dict()
                        for d in self._completable_old_in_flight(sub, sub.group, device)
                    ]
                else:
                    entry["reason"] = "still_on_old"
                blockers.append(entry)

            for sub in self._subs_of(device):
                if sub.channel != NEW or not sub.switch_through:
                    continue
                if not self._cursor_past(sub, *sub.switch_through):
                    blockers.append({
                        "kind": "group_behind_handoff",
                        "group": sub.group,
                        "through": {"epoch": sub.switch_through[0], "seq": sub.switch_through[1]},
                        "cursor": {"epoch": sub.cursor_epoch, "seq": sub.cursor_seq},
                    })

            if blockers:
                return {
                    "type": "retire",
                    "device": device,
                    "outcome": "blocked",
                    "blockers": blockers,
                }
            device_state.retired_at = timestamp
            device_state.retire_through = (through_epoch, through_seq)
            return {
                "type": "retire",
                "device": device,
                "outcome": "retired",
                "through": {"epoch": through_epoch, "seq": through_seq},
            }

    @staticmethod
    def _cursor_past(sub: Subscription, epoch: int, seq: int) -> bool:
        if sub.cursor_epoch is None:
            return False
        return (sub.cursor_epoch, sub.cursor_seq) > (epoch, seq)

    # ---------------------------------------------------------------- 查询

    def inspect(self, device: str, group: Optional[str] = None) -> dict[str, Any]:
        with self._lock:
            device_state = self.devices.get(device)
            if device_state is None:
                device_state = self._device(device)
            report: dict[str, Any] = {
                "type": "inspect",
                "device": device,
                "retired": device_state.retired_at is not None,
                "epochs": [],
                "summary": self._device_summary(device_state),
            }
            for epoch in sorted(device_state.epochs):
                current = device_state.epochs[epoch]
                positions_report = []
                for seq in sorted(current.positions):
                    position = current.positions[seq]
                    positions_report.append({
                        "epoch": epoch,
                        "seq": seq,
                        "state": position.state(),
                        "transports": sorted(position.records),
                        "delivered_transport": position.delivered_transport,
                        "post_delivery_conflicts": list(position.post_delivery_conflicts),
                    })
                report["epochs"].append({
                    "epoch": epoch,
                    "last_seq": current.last_seq,
                    "gaps": [list(gap) for gap in current.gaps],
                    "positions": positions_report,
                })
            if group is not None:
                report["group"] = self._group_report(group, device, device_state)
            else:
                report["groups"] = [
                    self._group_report(sub.group, device, device_state)
                    for sub in self._subs_of(device)
                ]
            return report

    def _device_summary(self, device_state: DeviceState) -> dict[str, Any]:
        conflicts: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        for epoch_state in device_state.epochs.values():
            # 冲突与交付后冲突证据不依赖代次是否完结，扫描全部已知位置。
            for seq, position in epoch_state.positions.items():
                if position.conflict:
                    conflicts.append({"epoch": epoch_state.epoch, "seq": seq})
            if epoch_state.last_seq is None:
                continue
            for seq in range(1, epoch_state.last_seq + 1):
                position = epoch_state.positions.get(seq)
                if epoch_state.declared_gap(seq):
                    continue
                if position is None or not position.has_content:
                    missing.append({"epoch": epoch_state.epoch, "seq": seq})
        return {
            "producer_conflicts": conflicts,
            "missing_events": missing,
        }

    def _group_report(
        self, group: str, device: str, device_state: DeviceState
    ) -> dict[str, Any]:
        sub = self.subscriptions.get((group, device))
        if sub is None:
            return {"group": group, "subscribed": False}
        deliveries = self._group_deliveries(group, device)
        in_flight = [d.to_dict() for d in deliveries.values() if d.status == HELD]
        reopenable = [
            d.to_dict()
            for d in deliveries.values()
            if d.status in (EXPIRED, GIVEBACK, CLOSED)
        ]
        classification = self._classify_group(sub, device, device_state, deliveries)
        return {
            "group": group,
            "subscribed": sub.unsubscribed_at is None,
            "channel": sub.channel,
            "switch_pending": sub.switch_pending,
            "switch_through": (
                {"epoch": sub.switch_through[0], "seq": sub.switch_through[1]}
                if sub.switch_through else None
            ),
            "switched_at": sub.switched_at,
            "cursor": {"epoch": sub.cursor_epoch, "seq": sub.cursor_seq},
            "in_flight": in_flight,
            "reopenable": reopenable,
            "classification": classification,
        }

    def _classify_group(
        self,
        sub: Subscription,
        device: str,
        device_state: DeviceState,
        deliveries: dict[str, Delivery],
    ) -> str:
        """区分：生产端冲突 / 暂缺事件 / 仍可完成的旧交付 / 单纯组进度慢。"""
        if any(d.status == HELD and d.channel == OLD for d in deliveries.values()):
            return "old_delivery_completable"
        if sub.cursor_epoch is not None:
            position = device_state.get_position(sub.cursor_epoch, sub.cursor_seq)
            if position is not None and position.conflict:
                return "producer_conflict"
            epoch_state = device_state.epochs.get(sub.cursor_epoch)
            if epoch_state is not None:
                # 游标越过代次末号：在等下一代次，属于正常追赶，不是缺事件。
                if epoch_state.last_seq is not None and sub.cursor_seq > epoch_state.last_seq:
                    return "catching_up"
                if not epoch_state.declared_gap(sub.cursor_seq) and (
                    position is None or not position.has_content
                ):
                    return "event_missing"
        return "catching_up"

    # ------------------------------------------------------------ 持久化支持

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "version": 1,
                "devices": [state.to_dict() for state in self.devices.values()],
                "subscriptions": [sub.to_dict() for sub in self.subscriptions.values()],
                "deliveries": [
                    {
                        "group": group,
                        "device": device,
                        "items": [d.to_dict() for d in mapping.values()],
                    }
                    for (group, device), mapping in self.deliveries.items()
                ],
            }

    @classmethod
    def restore(cls, raw: dict[str, Any]) -> "MigrationBus":
        bus = cls()
        for item in raw["devices"]:
            state = DeviceState.from_dict(item)
            bus.devices[state.device] = state
        for item in raw["subscriptions"]:
            sub = Subscription.from_dict(item)
            bus.subscriptions[(sub.group, sub.device)] = sub
        for block in raw["deliveries"]:
            mapping = {
                d["delivery_id"]: Delivery.from_dict(d) for d in block["items"]
            }
            bus.deliveries[(block["group"], block["device"])] = mapping
        return bus
