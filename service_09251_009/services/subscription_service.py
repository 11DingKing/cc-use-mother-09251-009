"""应用服务：订阅交付的核心编排。

职责：
- 合作方授权与密钥轮换；撤销授权即停止暴露并脱敏历史敏感字段
- 版本化过滤规则（字段 / 区域 / 拥堵等级 / 刷新频率各合作方不同）
- 从站点事件生成版本化增量包，游标驱动
- 交付租约：拉取占租约、确认推进游标、超时回收重投，顺序不被重排
- 计费与确认幂等：同一 (订阅, 事件) 不重复计费、不重复确认
- 运营方回放 / 暂停 / 审计
"""
from __future__ import annotations

import secrets
import sqlite3
import threading
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ..domain import models as m
from ..errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    RateLimitedError,
    SubscriptionPausedError,
    ValidationError,
)
from ..ports.clock import (
    Clock,
    IdGenerator,
    RandomIdGenerator,
    SystemClock,
)
from ..storage.sqlite_store import SQLiteStore

DEFAULT_LEASE_TTL = 30.0
SYSTEM_ACTOR = "system"


@dataclass
class FetchResult:
    batch: m.DeliveryBatch | None  # None 表示暂无新事件
    polled: bool


class SubscriptionService:
    def __init__(
        self,
        store: SQLiteStore,
        clock: Clock | None = None,
        id_gen: IdGenerator | None = None,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ) -> None:
        self.store = store
        self.clock = clock or SystemClock()
        self.ids = id_gen or RandomIdGenerator()
        self.lease_ttl = lease_ttl
        # 订阅级串行锁：同一订阅的构建/拉取串行化，避免并发产生重复批次
        self._fetch_locks: dict[str, threading.Lock] = {}
        self._fetch_locks_guard = threading.Lock()

    def _lock_for(self, subscription_id: str) -> threading.Lock:
        with self._fetch_locks_guard:
            lock = self._fetch_locks.get(subscription_id)
            if lock is None:
                lock = threading.Lock()
                self._fetch_locks[subscription_id] = lock
            return lock

    # ------------------------------------------------------------------ #
    # 事件摄入
    # ------------------------------------------------------------------ #

    def ingest_event(
        self,
        *,
        event_id: str,
        station_id: str,
        area: str,
        status: m.StationStatus,
        payload: Mapping[str, Any] | None = None,
        congestion: m.CongestionLevel = m.CongestionLevel.NORMAL,
        occurred_at: float | None = None,
    ) -> m.StationEvent | None:
        """幂等摄入站点事件。重复 event_id 返回 None，不会重复进入增量包。"""
        if not event_id or not station_id or not area:
            raise ValidationError("event_id/station_id/area 不能为空")
        now = self.clock.now()
        return self.store.insert_event(
            event_id=event_id,
            station_id=station_id,
            area=area,
            status=status,
            payload=dict(payload or {}),
            congestion=congestion,
            occurred_at=now if occurred_at is None else occurred_at,
            ingested_at=now,
        )

    # ------------------------------------------------------------------ #
    # 合作方与授权
    # ------------------------------------------------------------------ #

    def register_partner(self, partner_id: str, name: str) -> m.Partner:
        if self.store.get_partner(partner_id) is not None:
            raise ValidationError(f"合作方已存在: {partner_id}")
        now = self.clock.now()
        partner = m.Partner(partner_id=partner_id, name=name, created_at=now)
        self.store.insert_partner(partner)
        self._audit(m.AuditAction.PARTNER_CREATED, partner_id, None, {"name": name})
        return partner

    def issue_api_key(
        self, partner_id: str, *, expires_at: float | None = None
    ) -> tuple[str, str]:
        """签发密钥，返回 (key_id, 明文)。明文仅在返回时出现一次。"""
        partner = self._require_partner(partner_id)
        if partner.state is m.AuthState.REVOKED:
            raise AuthorizationError("授权已撤销，不能签发新密钥")
        key, secret = partner.issue_key(self.clock.now(), expires_at=expires_at)
        self.store.insert_api_key(partner_id, key)
        self._audit(
            m.AuditAction.KEY_ISSUED,
            partner_id,
            None,
            {"key_id": key.key_id, "expires_at": expires_at},
        )
        return key.key_id, secret

    def rotate_api_key(
        self, partner_id: str, *, expires_at: float | None = None
    ) -> dict[str, str]:
        """密钥轮换：签发新密钥并作废旧密钥；轮换期间调用方应先切换再收旧。

        返回 {"new_key_id", "new_secret", "revoked_key_id"}。
        """
        partner = self._require_partner(partner_id)
        now = self.clock.now()
        old_ids = [k.key_id for k in partner.keys.values() if k.valid_at(now)]
        new_key_id, new_secret = self.issue_api_key(partner_id, expires_at=expires_at)
        for old_id in old_ids:
            self.revoke_api_key(partner_id, old_id)
        return {
            "new_key_id": new_key_id,
            "new_secret": new_secret,
            "revoked_key_id": old_ids[0] if old_ids else "",
        }

    def revoke_api_key(self, partner_id: str, key_id: str) -> None:
        self._require_partner(partner_id)
        if self.store.revoke_api_key(key_id, self.clock.now()):
            self._audit(
                m.AuditAction.KEY_REVOKED, partner_id, None, {"key_id": key_id}
            )

    def revoke_partner(self, partner_id: str, reason: str = "") -> None:
        """撤销授权：拒绝一切访问，并脱敏该合作方全部历史批次的敏感字段。"""
        self._require_partner(partner_id)
        now = self.clock.now()
        self.store.update_partner_state(
            partner_id,
            m.AuthState.REVOKED,
            revoked_at=now,
            revoked_reason=reason,
        )
        stopped = self.store.revoke_live_batches(partner_id)
        redacted = self.store.redact_outbox_for_partner(partner_id)
        self._audit(
            m.AuditAction.PARTNER_REVOKED,
            partner_id,
            None,
            {
                "reason": reason,
                "stopped_batches": stopped,
                "redacted_items": redacted,
            },
        )

    def suspend_partner(self, partner_id: str, reason: str = "") -> None:
        """运营暂停：授权保留但停止交付。"""
        self._require_partner(partner_id)
        self.store.update_partner_state(partner_id, m.AuthState.SUSPENDED)
        self._audit(
            m.AuditAction.PARTNER_SUSPENDED, partner_id, None, {"reason": reason}
        )

    def resume_partner(self, partner_id: str) -> None:
        partner = self._require_partner(partner_id)
        if partner.state is not m.AuthState.SUSPENDED:
            raise ValidationError("仅暂停状态的合作方可以恢复")
        self.store.update_partner_state(partner_id, m.AuthState.ACTIVE)
        self._audit(m.AuditAction.PARTNER_RESUMED, partner_id, None, {})

    def authenticate(self, partner_id: str, secret: str) -> m.Partner:
        partner = self.store.get_partner(partner_id)
        if partner is None:
            raise AuthorizationError("合作方不存在")
        key = partner.authenticate(secret, self.clock.now())
        if key is None:
            raise AuthorizationError("密钥无效或已过期")
        if partner.state is m.AuthState.REVOKED:
            raise AuthorizationError("授权已撤销")
        if partner.state is m.AuthState.SUSPENDED:
            raise SubscriptionPausedError("订阅已被运营方暂停")
        return partner

    def _require_partner(self, partner_id: str) -> m.Partner:
        partner = self.store.get_partner(partner_id)
        if partner is None:
            raise NotFoundError(f"合作方不存在: {partner_id}")
        return partner

    # ------------------------------------------------------------------ #
    # 订阅与过滤规则
    # ------------------------------------------------------------------ #

    def create_subscription(
        self,
        partner_id: str,
        *,
        refresh_interval: float = 5.0,
        max_batch_size: int = 100,
        areas: Iterable[str] | None = None,
        fields: Iterable[str] | None = None,
        min_congestion: m.CongestionLevel = m.CongestionLevel.NORMAL,
        station_ids: Iterable[str] | None = None,
    ) -> m.Subscription:
        self._require_partner(partner_id)
        if refresh_interval < 0:
            raise ValidationError("刷新间隔不能为负")
        if max_batch_size <= 0:
            raise ValidationError("批量上限必须为正数")
        if fields is not None:
            unknown = set(fields) - m.KNOWN_FIELDS
            if unknown:
                raise ValidationError(f"未知字段: {sorted(unknown)}")
        sub_id = self.ids.new_id("sub")
        now = self.clock.now()
        rule = m.FilterRule(
            version=1,
            areas=frozenset(areas) if areas is not None else None,
            fields=frozenset(fields) if fields is not None else None,
            min_congestion=min_congestion,
            station_ids=frozenset(station_ids) if station_ids is not None else None,
            changed_at=now,
        )
        sub = m.Subscription(
            subscription_id=sub_id,
            partner_id=partner_id,
            refresh_interval=refresh_interval,
            max_batch_size=max_batch_size,
            rules=[rule],
        )
        self.store.insert_subscription(sub, now)
        self._audit(
            m.AuditAction.SUBSCRIPTION_CREATED,
            partner_id,
            sub_id,
            {
                "refresh_interval": refresh_interval,
                "max_batch_size": max_batch_size,
                "areas": sorted(rule.areas) if rule.areas else None,
                "fields": sorted(rule.fields) if rule.fields else None,
            },
        )
        return sub

    def adjust_rule(
        self,
        subscription_id: str,
        *,
        areas: Any = m.KEEP,
        fields: Any = m.KEEP,
        min_congestion: Any = m.KEEP,
        station_ids: Any = m.KEEP,
        reason: str = "",
    ) -> m.FilterRule:
        """区域/字段调整：产生新版本规则；存量批次仍按生成时版本交付。

        缺省维度维持上一版本；显式传 None 表示放开为全部区域/字段。
        """
        sub = self._require_subscription(subscription_id)
        if fields is not m.KEEP and fields is not None:
            unknown = set(fields) - m.KNOWN_FIELDS
            if unknown:
                raise ValidationError(f"未知字段: {sorted(unknown)}")
        rule = sub.adjust(
            self.clock.now(),
            areas=areas,
            fields=fields,
            min_congestion=min_congestion,
            station_ids=station_ids,
            reason=reason,
        )
        self.store.insert_filter_rule(subscription_id, rule)
        self._audit(
            m.AuditAction.RULE_ADJUSTED,
            sub.partner_id,
            subscription_id,
            {
                "version": rule.version,
                "areas": sorted(rule.areas) if rule.areas else None,
                "fields": sorted(rule.fields) if rule.fields else None,
                "min_congestion": rule.min_congestion.value,
                "reason": reason,
            },
        )
        return rule

    def _require_subscription(self, subscription_id: str) -> m.Subscription:
        sub = self.store.get_subscription(subscription_id)
        if sub is None:
            raise NotFoundError(f"订阅不存在: {subscription_id}")
        return sub

    # ------------------------------------------------------------------ #
    # 拉取 / 租约
    # ------------------------------------------------------------------ #

    def fetch_batch(
        self,
        partner_id: str,
        secret: str,
        subscription_id: str,
        *,
        max_items: int | None = None,
    ) -> FetchResult:
        """合作方拉取增量包。

        - 频率受订阅 refresh_interval 约束（租约回收重试不受限）
        - 存在未过期租约时原样返回同一批次（幂等拉取，不重复计费）
        - 租约已过期则顺序重投同一批次，delivery_attempt +1
        """
        partner = self.authenticate(partner_id, secret)
        sub = self._require_subscription(subscription_id)
        if sub.partner_id != partner.partner_id:
            raise AuthorizationError("订阅不属于该合作方")

        with self._lock_for(subscription_id):
            now = self.clock.now()

            # 先回收本订阅已过期的租约
            self.store.expire_leases(now)
            outstanding = self.store.outstanding_batch(subscription_id)

            if outstanding is not None:
                # 租约内幂等返回或超时重投均属重试，不刷新刷新节奏计时
                return self._lease_existing(sub, partner, outstanding, now)

            self._enforce_cadence(sub, now)
            try:
                result = self._build_and_lease(
                    sub, partner, now, max_items=max_items
                )
            except sqlite3.IntegrityError:
                # 并发兜底：另一线程已创建存活批次，回读复用
                outstanding = self.store.outstanding_batch(subscription_id)
                if outstanding is None:
                    raise
                return self._lease_existing(sub, partner, outstanding, now)

            # 新内容消费才刷新节奏（即使本次为空，也计一次轮询）
            self.store.set_last_pull(subscription_id, now)
            return result

    def _enforce_cadence(self, sub: m.Subscription, now: float) -> None:
        last = self.store.get_last_pull(sub.subscription_id)
        if last is not None and now - last < sub.refresh_interval:
            raise RateLimitedError(
                f"超过订阅刷新频率：最短间隔 {sub.refresh_interval}s"
            )

    def _lease_existing(
        self,
        sub: m.Subscription,
        partner: m.Partner,
        batch: m.DeliveryBatch,
        now: float,
    ) -> FetchResult:
        if batch.state is m.LeaseState.DELIVERED:
            if batch.lease_expires_at is not None and batch.lease_expires_at > now:
                # 租约有效：幂等返回，不重复计费、不换所有者
                return FetchResult(batch=batch, polled=True)
            # 理论上 expire_leases 已处理，防御性兜底
        # PENDING（发件箱待投递）/ EXPIRED（超时重投）-> 占租约交付，保持原有序列
        return FetchResult(batch=self._deliver(batch, partner, now), polled=True)

    def _build_pending_batch(
        self,
        sub: m.Subscription,
        partner: m.Partner,
        now: float,
        *,
        max_items: int | None,
    ) -> m.DeliveryBatch | None:
        """按当前游标与规则生成 PENDING 增量包入发件箱（不占租约、不计费）。"""
        cursor = self.store.get_cursor(sub.subscription_id)
        rule = sub.current_rule
        limit = min(sub.max_batch_size, max_items or sub.max_batch_size)
        events: list[m.StationEvent] = []
        for event in self.store.events_after(cursor.last_acked_seq):
            if rule.accepts(event):
                events.append(event)
                if len(events) >= limit:
                    break
        if not events:
            return None

        redact = partner.state is m.AuthState.REVOKED
        items = tuple(
            m.BatchItem(
                event=event,
                projection=event.project(rule.fields, redact_sensitive=redact),
                billing_fingerprint=m.billing_fingerprint(
                    sub.subscription_id, event.event_id
                ),
            )
            for event in events
        )
        priority = max((event.congestion.priority for event in events), default=0)
        batch = m.DeliveryBatch(
            batch_id=self.ids.new_id("batch"),
            subscription_id=sub.subscription_id,
            delivery_attempt=1,
            from_seq=cursor.last_acked_seq,
            to_seq=events[-1].seq,
            items=items,
            rule_version=rule.version,
            state=m.LeaseState.PENDING,
            created_at=now,
            priority=priority,
            redacted=redact,
        )
        self.store.insert_batch(batch)
        return batch

    def _build_and_lease(
        self,
        sub: m.Subscription,
        partner: m.Partner,
        now: float,
        *,
        max_items: int | None,
    ) -> FetchResult:
        batch = self._build_pending_batch(sub, partner, now, max_items=max_items)
        if batch is None:
            return FetchResult(batch=None, polled=True)
        return FetchResult(batch=self._deliver(batch, partner, now), polled=True)

    def _deliver(
        self, batch: m.DeliveryBatch, partner: m.Partner, now: float
    ) -> m.DeliveryBatch:
        """占用交付租约并记账。计费按指纹去重，重投/回放不重复计费。"""
        owner = f"{partner.partner_id}:{secrets.token_hex(8)}"
        redelivery = batch.state is m.LeaseState.EXPIRED
        attempt = batch.delivery_attempt + (1 if redelivery else 0)
        expires = now + self.lease_ttl
        self.store.update_batch_lease(
            batch.batch_id,
            state=m.LeaseState.DELIVERED,
            attempt=attempt,
            owner=owner,
            leased_at=now,
            expires_at=expires,
        )
        charged = self.store.record_billing(
            (
                item.billing_fingerprint,
                batch.subscription_id,
                item.event.event_id,
                batch.batch_id,
                now,
            )
            for item in batch.items
        )
        self._audit(
            m.AuditAction.BATCH_REDELIVERED if redelivery else m.AuditAction.BATCH_FETCHED,
            partner.partner_id,
            batch.subscription_id,
            {
                "batch_id": batch.batch_id,
                "attempt": attempt,
                "events": len(batch.items),
                "new_charges": charged,
                "priority": batch.priority,
            },
        )
        return self.store.get_batch(batch.batch_id)  # type: ignore[return-value]

    def materialize_due(self) -> list[m.DeliveryBatch]:
        """推送侧：为所有无存活批次的活跃订阅生成 PENDING 增量包入发件箱。

        突发拥堵批次 priority 更高，delivery_queue 中抢占队首。
        """
        created: list[m.DeliveryBatch] = []
        now = self.clock.now()
        self.store.expire_leases(now)
        for sub in self.store.list_subscriptions():
            partner = self.store.get_partner(sub.partner_id)
            if partner is None or not partner.active:
                continue
            with self._lock_for(sub.subscription_id):
                if self.store.outstanding_batch(sub.subscription_id) is not None:
                    continue
                batch = self._build_pending_batch(
                    sub, partner, now, max_items=None
                )
                if batch is not None:
                    created.append(batch)
        return created

    def delivery_queue(self) -> list[m.DeliveryBatch]:
        """待交付队列（高拥堵优先，同级 FIFO）。"""
        self.store.expire_leases(self.clock.now())
        return self.store.delivery_queue()

    def dispatch_next(self) -> m.DeliveryBatch | None:
        """推送出口：按优先级弹出队首批次并占用交付租约。

        突发拥堵批次优先送达；无待投递项时返回 None。与拉取路径共用同一套
        发件箱、租约与计费去重，因此同一批次不会因"又推又拉"重复计费。
        """
        queue = self.delivery_queue()
        if not queue:
            return None
        head = queue[0]
        with self._lock_for(head.subscription_id):
            # 加锁后回读：可能已被并发的拉取/派发占租约
            batch = self.store.get_batch(head.batch_id)
            if batch is None:
                return None
            if batch.state is m.LeaseState.DELIVERED:
                # 已被并发拉取占租约，推送侧跳过（租约确认或超时后会再调度）
                return None
            if batch.state not in (m.LeaseState.PENDING, m.LeaseState.EXPIRED):
                return None
            partner = self.store.get_partner(
                self._require_subscription(batch.subscription_id).partner_id
            )
            return self._deliver(batch, partner, self.clock.now())

    # ------------------------------------------------------------------ #
    # 确认
    # ------------------------------------------------------------------ #

    def ack_batch(
        self,
        partner_id: str,
        secret: str,
        subscription_id: str,
        batch_id: str,
        lease_owner: str,
    ) -> m.Cursor:
        """确认批次：仅当前租约持有者可确认，游标单调推进。

        - 重复确认已 ACKED 批次：幂等返回当前游标，不重复推进/计费/审计
        - 租约过期或被回放作废后持旧令牌确认：拒绝（ConflictError）
        """
        partner = self.authenticate(partner_id, secret)
        sub = self._require_subscription(subscription_id)
        if sub.partner_id != partner.partner_id:
            raise AuthorizationError("订阅不属于该合作方")

        batch = self.store.get_batch(batch_id)
        if batch is None or batch.subscription_id != subscription_id:
            raise NotFoundError("批次不存在")

        if batch.state is m.LeaseState.ACKED:
            # 仅原确认者的迟到重试幂等成功；持旧/伪造令牌者一律拒绝
            if batch.lease_owner == lease_owner:
                return self.store.get_cursor(subscription_id)
            raise ConflictError("批次已由其他租约确认，旧令牌无效")
        non_ackable = (
            m.LeaseState.EXPIRED,
            m.LeaseState.SUPERSEDED,
            m.LeaseState.PENDING,
            m.LeaseState.REVOKED,
        )
        if batch.state in non_ackable:
            raise ConflictError(
                f"批次当前状态 {batch.state.value}，不能确认；请重新拉取"
            )
        if (
            batch.lease_expires_at is not None
            and batch.lease_expires_at <= self.clock.now()
        ):
            # 租约已到期（回收器可能尚未运行），旧持有者的迟到确认必须拒绝
            raise ConflictError("租约已过期，确认被拒绝；请重新拉取")

        advanced_to = self.store.ack_batch_atomic(
            batch_id, lease_owner, self.clock.now()
        )
        if advanced_to is None:
            # 可能在预检与原子更新之间被同属主的并发重试抢先提交；回读甄别
            latest = self.store.get_batch(batch_id)
            if (
                latest is not None
                and latest.state is m.LeaseState.ACKED
                and latest.lease_owner == lease_owner
            ):
                return self.store.get_cursor(subscription_id)
            raise ConflictError("租约所有者不匹配或租约已失效，确认被拒绝")

        # 幂等防御：对本批指纹再记账一次也不会重复
        self.store.record_billing(
            (
                item.billing_fingerprint,
                batch.subscription_id,
                item.event.event_id,
                batch.batch_id,
                self.clock.now(),
            )
            for item in batch.items
        )
        self._audit(
            m.AuditAction.BATCH_ACKED,
            partner.partner_id,
            subscription_id,
            {
                "batch_id": batch_id,
                "attempt": batch.delivery_attempt,
                "advanced_to": advanced_to,
                "events": len(batch.items),
            },
        )
        return self.store.get_cursor(subscription_id)

    # ------------------------------------------------------------------ #
    # 运营：回放 / 暂停 / 租约回收 / 审计
    # ------------------------------------------------------------------ #

    def replay(
        self,
        subscription_id: str,
        *,
        from_seq: int = 0,
        actor: str = "operator",
    ) -> None:
        """回放：未确认旧批次作废，游标回退到 from_seq，下次拉取重新交付。

        已计费指纹不重复计费；若合作方已撤销，重放包按脱敏规则生成。
        """
        sub = self._require_subscription(subscription_id)
        if from_seq < 0:
            raise ValidationError("from_seq 不能为负")
        now = self.clock.now()
        superseded = self.store.supersede_unacked(subscription_id)
        self.store.reset_cursor(subscription_id, from_seq, now)
        self._audit(
            m.AuditAction.CURSOR_RESET,
            sub.partner_id,
            subscription_id,
            {"from_seq": from_seq, "superseded_batches": superseded, "actor": actor},
        )
        self._audit(
            m.AuditAction.BATCH_REPLAYED,
            sub.partner_id,
            subscription_id,
            {"from_seq": from_seq, "superseded_batches": superseded, "actor": actor},
        )

    def expire_leases(self) -> list[str]:
        """定时维护：回收超时租约，回收后的批次按原顺序重投。"""
        return self.store.expire_leases(self.clock.now())

    def audit_trail(
        self,
        *,
        subscription_id: str | None = None,
        partner_id: str | None = None,
        action: m.AuditAction | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return self.store.list_audit(
            subscription_id=subscription_id,
            partner_id=partner_id,
            action=action,
            limit=limit,
        )

    # ------------------------------------------------------------------ #

    def _audit(
        self,
        action: m.AuditAction,
        partner_id: str | None,
        subscription_id: str | None,
        detail: Mapping[str, Any],
    ) -> None:
        self.store.append_audit(
            m.AuditEntry(
                audit_id=None,
                at=self.clock.now(),
                action=action,
                partner_id=partner_id,
                subscription_id=subscription_id,
                actor=SYSTEM_ACTOR,
                detail=dict(detail),
            )
        )
