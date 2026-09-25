"""领域模型：充电事件、合作方、订阅、过滤规则、批次、游标、审计。"""
from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Iterable, Mapping


# 规则调整哨兵：KEEP 表示该维度维持上一版本（区别于显式 None = 放开为全部）
class _Keep:
    _instance: "_Keep | None" = None

    def __new__(cls) -> "_Keep":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "KEEP"

    def __bool__(self) -> bool:
        raise TypeError("KEEP 不能作为布尔值使用")


KEEP = _Keep()

# ---------------------------------------------------------------------------
# 事件与区域
# ---------------------------------------------------------------------------

# 敏感字段：授权撤销后，历史批次回放时必须脱敏
SENSITIVE_FIELDS = frozenset({"operator_note", "internal_code", "repair_contact"})

# 全部可下发字段，越界字段不会出现在增量包中
KNOWN_FIELDS = frozenset(
    {
        "station_id",
        "area",
        "status",
        "available_connectors",
        "total_connectors",
        "queue_length",
        "congestion_level",
        "updated_at",
        *SENSITIVE_FIELDS,
    }
)


class StationStatus(str, Enum):
    AVAILABLE = "available"
    CONGESTED = "congested"
    CHARGING = "charging"
    FAULT = "fault"
    OFFLINE = "offline"


class CongestionLevel(str, Enum):
    """突发拥堵等级；HIGH 在交付时享有抢占式优先级。"""

    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def priority(self) -> int:
        return {"normal": 0, "high": 1, "critical": 2}[self.value]


def _redacted() -> dict[str, Any]:
    return {name: None for name in SENSITIVE_FIELDS}


@dataclass(frozen=True)
class StationEvent:
    """站点状态变化事件（事实，不可变）。"""

    seq: int  # 全局单调序列号，由事件存储分配
    event_id: str  # 幂等键：同一物理事件重复摄入只产生一条
    station_id: str
    area: str
    status: StationStatus
    payload: Mapping[str, Any]
    congestion: CongestionLevel = CongestionLevel.NORMAL
    occurred_at: float = 0.0
    ingested_at: float = 0.0

    def to_full_dict(self) -> dict[str, Any]:
        data = {
            "event_id": self.event_id,
            "seq": self.seq,
            "station_id": self.station_id,
            "area": self.area,
            "status": self.status.value,
            "congestion": self.congestion.value,
            "occurred_at": self.occurred_at,
            **dict(self.payload),
        }
        return data

    def project(
        self,
        fields: Iterable[str] | None,
        redact_sensitive: bool,
    ) -> dict[str, Any]:
        """按合作方可接收字段投影；授权失效时对敏感字段脱敏。"""
        data = self.to_full_dict()
        allowed = set(fields) if fields is not None else set(KNOWN_FIELDS)
        allowed &= KNOWN_FIELDS
        # 身份字段始终保留，否则下游无法关联
        allowed |= {"event_id", "seq"}
        out = {k: v for k, v in data.items() if k in allowed}
        if redact_sensitive:
            out.update({k: None for k in SENSITIVE_FIELDS if k in allowed})
        return out


# ---------------------------------------------------------------------------
# 合作方、密钥与授权
# ---------------------------------------------------------------------------

class AuthState(str, Enum):
    ACTIVE = "active"
    REVOKED = "revoked"
    SUSPENDED = "suspended"  # 运营暂停：暂停拉取，但授权仍在


@dataclass
class ApiKey:
    """合作方 API 密钥（仅存哈希）。支持轮换：同一时刻可有两个有效密钥。"""

    key_id: str
    secret_hash: str
    created_at: float
    expires_at: float | None = None
    revoked_at: float | None = None

    def valid_at(self, now: float) -> bool:
        if self.revoked_at is not None:
            return False
        if self.expires_at is not None and now >= self.expires_at:
            return False
        return True

    @staticmethod
    def hash_secret(secret: str) -> str:
        return hashlib.sha256(secret.encode("utf-8")).hexdigest()

    @staticmethod
    def new_secret(n: int = 32) -> str:
        return secrets.token_urlsafe(n)


@dataclass
class Partner:
    partner_id: str
    name: str
    state: AuthState = AuthState.ACTIVE
    keys: dict[str, ApiKey] = field(default_factory=dict)
    created_at: float = 0.0
    revoked_at: float | None = None
    revoked_reason: str | None = None

    @property
    def active(self) -> bool:
        return self.state is AuthState.ACTIVE

    def issue_key(self, now: float, expires_at: float | None = None) -> tuple[ApiKey, str]:
        """签发新密钥，返回 (密钥记录, 明文)。明文仅此一次可见。"""
        secret = ApiKey.new_secret()
        key = ApiKey(
            key_id=f"key_{secrets.token_hex(8)}",
            secret_hash=ApiKey.hash_secret(secret),
            created_at=now,
            expires_at=expires_at,
        )
        self.keys[key.key_id] = key
        return key, secret

    def authenticate(self, secret: str, now: float) -> ApiKey | None:
        candidate = ApiKey.hash_secret(secret)
        for key in self.keys.values():
            if key.valid_at(now) and hmac.compare_digest(key.secret_hash, candidate):
                return key
        return None


# ---------------------------------------------------------------------------
# 订阅、过滤规则与版本化
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FilterRule:
    """合作方的过滤配置。规则版本化，调整区域/字段会产生新版本。"""

    version: int
    areas: frozenset[str] | None  # None = 全部区域
    fields: frozenset[str] | None  # None = 全部已知字段
    min_congestion: CongestionLevel = CongestionLevel.NORMAL
    station_ids: frozenset[str] | None = None  # 可选白名单
    changed_at: float = 0.0
    reason: str = ""

    def accepts(self, event: StationEvent) -> bool:
        if self.areas is not None and event.area not in self.areas:
            return False
        if self.station_ids is not None and event.station_id not in self.station_ids:
            return False
        if event.congestion.priority < self.min_congestion.priority:
            return False
        return True


@dataclass
class Subscription:
    subscription_id: str
    partner_id: str
    refresh_interval: float  # 建议拉取频率（秒），最小节奏约束
    max_batch_size: int = 100
    rules: list[FilterRule] = field(default_factory=list)  # 按版本升序

    @property
    def current_rule(self) -> FilterRule:
        return self.rules[-1]

    def rule_at(self, rule_version: int) -> FilterRule | None:
        for rule in self.rules:
            if rule.version == rule_version:
                return rule
        return None

    def adjust(
        self,
        now: float,
        *,
        areas: Any = KEEP,
        fields: Any = KEEP,
        min_congestion: Any = KEEP,
        station_ids: Any = KEEP,
        reason: str = "",
    ) -> FilterRule:
        """区域/字段调整：基于当前规则生成新版本，旧版本保留以便审计与回溯。

        语义：KEEP 表示不变；None 表示放开为全部；可迭代对象表示收窄为集合。
        """
        cur = self.current_rule

        def resolve(current: Any, given: Any) -> Any:
            if given is KEEP:
                return current
            if given is None:
                return None
            return frozenset(given)

        new_rule = FilterRule(
            version=cur.version + 1,
            areas=resolve(cur.areas, areas),
            fields=resolve(cur.fields, fields),
            min_congestion=cur.min_congestion if min_congestion is KEEP else min_congestion,
            station_ids=resolve(cur.station_ids, station_ids),
            changed_at=now,
            reason=reason,
        )
        self.rules.append(new_rule)
        return new_rule


# ---------------------------------------------------------------------------
# 游标、批次与交付租约
# ---------------------------------------------------------------------------

class LeaseState(str, Enum):
    PENDING = "pending"      # 已生成，待拉取/投递
    DELIVERED = "delivered"  # 已被拉取，持有租约待确认
    ACKED = "acked"          # 已确认，游标推进
    EXPIRED = "expired"      # 租约超时，可重新交付
    SUPERSEDED = "superseded"  # 回放后旧批次作废，不再投递
    REVOKED = "revoked"      # 授权撤销，立即停止投递


@dataclass(frozen=True)
class Cursor:
    """订阅消费游标。last_acked_seq 之前的事件均已确认。"""

    subscription_id: str
    last_acked_seq: int = 0
    updated_at: float = 0.0


@dataclass
class DeliveryBatch:
    """一次增量交付包。包内事件按 seq 升序，保证顺序。"""

    batch_id: str
    subscription_id: str
    delivery_attempt: int
    from_seq: int            # 游标视角：(last_acked_seq, to_seq]
    to_seq: int
    items: tuple[BatchItem, ...]
    rule_version: int
    lease_owner: str | None = None
    leased_at: float | None = None
    lease_expires_at: float | None = None
    state: LeaseState = LeaseState.PENDING
    created_at: float = 0.0
    # 高拥堵批次抢占优先级（越大越优先）
    priority: int = 0
    redacted: bool = False

    @property
    def event_ids(self) -> tuple[str, ...]:
        return tuple(item.event.event_id for item in self.items)

    def to_wire(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "subscription_id": self.subscription_id,
            "delivery_attempt": self.delivery_attempt,
            "from_seq": self.from_seq,
            "to_seq": self.to_seq,
            "rule_version": self.rule_version,
            "state": self.state.value,
            "priority": self.priority,
            "redacted": self.redacted,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at,
            "events": [item.to_wire() for item in self.items],
        }


@dataclass(frozen=True)
class BatchItem:
    """批次内单条事件投影；记录计费指纹以防重复计费。"""

    event: StationEvent
    projection: Mapping[str, Any]
    billing_fingerprint: str

    def to_wire(self) -> dict[str, Any]:
        return dict(self.projection)


def billing_fingerprint(subscription_id: str, event_id: str) -> str:
    """一次 (订阅, 事件) 交付的计费指纹；确认/重投都不应重复计费。"""
    h = hashlib.sha256()
    h.update(subscription_id.encode())
    h.update(b"|")
    h.update(event_id.encode())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# 审计
# ---------------------------------------------------------------------------

class AuditAction(str, Enum):
    PARTNER_CREATED = "partner_created"
    KEY_ISSUED = "key_issued"
    KEY_REVOKED = "key_revoked"
    PARTNER_REVOKED = "partner_revoked"
    PARTNER_SUSPENDED = "partner_suspended"
    PARTNER_RESUMED = "partner_resumed"
    SUBSCRIPTION_CREATED = "subscription_created"
    RULE_ADJUSTED = "rule_adjusted"
    BATCH_FETCHED = "batch_fetched"
    BATCH_ACKED = "batch_acked"
    BATCH_REDELIVERED = "batch_redelivered"
    BATCH_REPLAYED = "batch_replayed"
    CURSOR_RESET = "cursor_reset"
    BILLING_RECORDED = "billing_recorded"


@dataclass(frozen=True)
class AuditEntry:
    audit_id: int | None
    at: float
    action: AuditAction
    partner_id: str | None
    subscription_id: str | None
    actor: str
    detail: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "at": self.at,
            "action": self.action.value,
            "partner_id": self.partner_id,
            "subscription_id": self.subscription_id,
            "actor": self.actor,
            "detail": dict(self.detail),
        }


__all__ = [
    "KEEP",
    "SENSITIVE_FIELDS",
    "KNOWN_FIELDS",
    "StationStatus",
    "CongestionLevel",
    "StationEvent",
    "AuthState",
    "ApiKey",
    "Partner",
    "FilterRule",
    "Subscription",
    "LeaseState",
    "Cursor",
    "DeliveryBatch",
    "BatchItem",
    "billing_fingerprint",
    "AuditAction",
    "AuditEntry",
    "replace",
]
