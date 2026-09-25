"""应用服务：订阅交付的全部业务规则。

- 合作方授权：注册、停用、撤销，API 密钥轮换（宽限期）与吊销；
- 过滤规则：区域白名单、字段白名单、刷新频率、批次大小、租约时长；
- 版本化增量包：站点事件写入全局递增版本日志，按订阅游标切批次；
- 游标与交付租约：拉取建租约、确认推进游标、租约过期同批次顺序重投；
- 幂等：同一事件不重复入日志、不重复计费，确认操作可安全重试；
- 运营：回放、暂停、审计。
"""
from __future__ import annotations

import hmac
from typing import Any

from .domain import (
    ALL_FIELDS,
    BATCH_ACKED,
    BATCH_LEASED,
    BATCH_SUPERSEDED,
    KEY_ACTIVE,
    KEY_RETIRING,
    KEY_REVOKED,
    PARTNER_ACTIVE,
    PARTNER_SUSPENDED,
    PRIORITY_HIGH,
    SUB_PAUSED,
    SUB_REVOKED,
    AuthError,
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ValidationError,
    hash_key,
    project_fields,
)
from .ports import Clock, IdGenerator, SecretKeyProvider, SystemClock, UuidIds
from .storage import Storage


class SubscriptionService:
    """订阅交付应用服务（线程安全，状态全部落 SQLite）。"""

    def __init__(
        self,
        storage: Storage,
        clock: Clock | None = None,
        ids: IdGenerator | None = None,
    ) -> None:
        self.db = storage
        self.clock = clock or SystemClock()
        self.ids = ids or UuidIds()
        self._keys = SecretKeyProvider()

    # ================= 合作方与密钥 =================

    def register_partner(self, name: str, *, actor: str = "ops") -> dict[str, Any]:
        """注册合作方并签发首把 API 密钥（明文仅本次返回）。"""
        if not name:
            raise ValidationError("合作方名称不能为空")
        now = self.clock.now()
        partner_id = self.ids.new_id("p")
        api_key = self._keys.generate_key()
        with self.db.transaction():
            self.db.insert_partner({
                "id": partner_id, "name": name,
                "status": PARTNER_ACTIVE, "created_at": now,
            })
            self.db.insert_key({
                "id": self.ids.new_id("k"), "partner_id": partner_id,
                "key_hash": hash_key(api_key), "label": "initial",
                "status": KEY_ACTIVE, "created_at": now, "not_after": None,
            })
            self.db.insert_audit({
                "at": now, "actor": actor, "action": "partner.register",
                "target_type": "partner", "target_id": partner_id,
                "detail": {"name": name},
            })
        return {"partner_id": partner_id, "name": name, "api_key": api_key}

    def suspend_partner(self, partner_id: str, *, actor: str = "ops") -> None:
        self._set_partner_status(partner_id, PARTNER_SUSPENDED, "partner.suspend", actor)

    def reactivate_partner(self, partner_id: str, *, actor: str = "ops") -> None:
        self._set_partner_status(partner_id, PARTNER_ACTIVE, "partner.reactivate", actor)

    def _set_partner_status(
        self, partner_id: str, status: str, action: str, actor: str
    ) -> None:
        now = self.clock.now()
        self._partner(partner_id)
        with self.db.transaction():
            self.db.update_partner_status(partner_id, status)
            self.db.insert_audit({
                "at": now, "actor": actor, "action": action,
                "target_type": "partner", "target_id": partner_id,
                "detail": {"status": status},
            })

    def rotate_key(
        self, partner_id: str, *, grace_seconds: float = 3600.0,
        label: str = "rotated", actor: str = "ops",
    ) -> dict[str, str]:
        """密钥轮换：签发新密钥，旧密钥进入宽限期（retiring），到期自动失效。"""
        if grace_seconds < 0:
            raise ValidationError("grace_seconds 不能为负")
        now = self.clock.now()
        self._partner(partner_id)
        new_key = self._keys.generate_key()
        with self.db.transaction():
            for key in self.db.list_keys(partner_id, (KEY_ACTIVE, KEY_RETIRING)):
                self.db.update_key(
                    key["id"], status=KEY_RETIRING,
                    not_after=key["not_after"] or now + grace_seconds,
                )
            self.db.insert_key({
                "id": self.ids.new_id("k"), "partner_id": partner_id,
                "key_hash": hash_key(new_key), "label": label,
                "status": KEY_ACTIVE, "created_at": now, "not_after": None,
            })
            self.db.insert_audit({
                "at": now, "actor": actor, "action": "key.rotate",
                "target_type": "partner", "target_id": partner_id,
                "detail": {"grace_seconds": grace_seconds, "label": label},
            })
        return {"partner_id": partner_id, "api_key": new_key}

    def revoke_key(self, key_id: str, *, actor: str = "ops") -> None:
        """立即吊销密钥。"""
        now = self.clock.now()
        row = self.db.get_key(key_id)
        if row is None:
            raise NotFoundError("密钥不存在")
        with self.db.transaction():
            self.db.update_key(key_id, status=KEY_REVOKED)
            self.db.insert_audit({
                "at": now, "actor": actor, "action": "key.revoke",
                "target_type": "key", "target_id": key_id,
                "detail": {"partner_id": row["partner_id"]},
            })

    def list_keys(self, partner_id: str) -> list[dict[str, Any]]:
        """列出合作方密钥元数据（不含散列）。"""
        self._partner(partner_id)
        return [
            {
                "id": k["id"], "label": k["label"], "status": k["status"],
                "created_at": k["created_at"], "not_after": k["not_after"],
            }
            for k in self.db.list_keys(partner_id)
        ]

    def authenticate(self, api_key: str) -> dict[str, Any]:
        """校验 API 密钥并返回合作方。常时间比较，避免时序探测。"""
        candidate = hash_key(api_key)
        row = self.db.get_key_by_hash(candidate)
        if row is None:
            hmac.compare_digest(candidate, "0" * 64)  # 对齐不存在的路径耗时
            raise AuthError("API 密钥无效")
        now = self.clock.now()
        if row["status"] == KEY_REVOKED or (
            row["not_after"] is not None and now > row["not_after"]
        ):
            raise AuthError("API 密钥已失效")
        partner = self.db.get_partner(row["partner_id"])
        if partner is None or partner["status"] != PARTNER_ACTIVE:
            raise ForbiddenError("合作方已停用")
        return partner

    # ================= 订阅（过滤规则） =================

    def create_subscription(
        self,
        partner_id: str,
        name: str,
        *,
        regions: list[str],
        fields: list[str] | None = None,
        min_interval_seconds: float = 0.0,
        max_batch_size: int = 100,
        lease_seconds: float = 300.0,
        actor: str = "ops",
    ) -> dict[str, Any]:
        self._validate_rules(regions, fields, min_interval_seconds, max_batch_size, lease_seconds)
        self._partner(partner_id)
        now = self.clock.now()
        sub_id = self.ids.new_id("s")
        row = {
            "id": sub_id, "partner_id": partner_id, "name": name,
            "status": "active", "regions": list(regions),
            "fields": list(fields) if fields is not None else [ALL_FIELDS],
            "min_interval_seconds": float(min_interval_seconds),
            "max_batch_size": int(max_batch_size),
            "lease_seconds": float(lease_seconds),
            "cursor_version": 0, "last_pull_at": None,
            "created_at": now, "updated_at": now,
        }
        with self.db.transaction():
            self.db.insert_subscription(row)
            self.db.insert_audit({
                "at": now, "actor": actor, "action": "subscription.create",
                "target_type": "subscription", "target_id": sub_id,
                "detail": {"name": name, "regions": row["regions"], "fields": row["fields"]},
            })
        return self.get_subscription(sub_id, partner_id)

    @staticmethod
    def _validate_rules(
        regions: list[str], fields: list[str] | None,
        min_interval_seconds: float, max_batch_size: int, lease_seconds: float,
    ) -> None:
        if not regions or any(not isinstance(r, str) or not r for r in regions):
            raise ValidationError("至少配置一个有效区域")
        if len(set(regions)) != len(regions):
            raise ValidationError("区域不可重复")
        if fields is not None:
            if not fields or any(not isinstance(f, str) or not f for f in fields):
                raise ValidationError("字段白名单不可为空")
            if ALL_FIELDS in fields and len(fields) > 1:
                raise ValidationError("'*' 不能与具体字段混用")
        if min_interval_seconds < 0:
            raise ValidationError("刷新频率不可为负")
        if max_batch_size < 1:
            raise ValidationError("批次大小至少为 1")
        if lease_seconds <= 0:
            raise ValidationError("租约时长必须为正")

    def get_subscription(self, subscription_id: str, partner_id: str) -> dict[str, Any]:
        sub = self.db.get_subscription(subscription_id)
        # 他人订阅与不存在走同一响应，不暴露订阅的存在性。
        if sub is None or sub["partner_id"] != partner_id:
            raise NotFoundError("订阅不存在")
        return sub

    def update_rules(
        self,
        subscription_id: str,
        *,
        regions: list[str] | None = None,
        fields: list[str] | None = None,
        min_interval_seconds: float | None = None,
        max_batch_size: int | None = None,
        lease_seconds: float | None = None,
        actor: str = "ops",
    ) -> dict[str, Any]:
        """调整过滤规则。

        区域调整立即生效：已租出未确认的批次作废，游标之后按新区域重新切包；
        字段调整同样立即生效：在途批次重投时按新白名单渲染。
        """
        sub = self.db.get_subscription(subscription_id)
        if sub is None:
            raise NotFoundError("订阅不存在")
        new_regions = sub["regions"] if regions is None else regions
        new_fields = sub["fields"] if fields is None else fields
        new_interval = sub["min_interval_seconds"] if min_interval_seconds is None else min_interval_seconds
        new_size = sub["max_batch_size"] if max_batch_size is None else max_batch_size
        new_lease = sub["lease_seconds"] if lease_seconds is None else lease_seconds
        self._validate_rules(new_regions, new_fields, new_interval, new_size, new_lease)
        now = self.clock.now()
        region_changed = list(new_regions) != sub["regions"]
        with self.db.transaction():
            self.db.update_subscription(
                subscription_id, regions=new_regions, fields=new_fields,
                min_interval_seconds=new_interval, max_batch_size=new_size,
                lease_seconds=new_lease, updated_at=now,
            )
            if region_changed:
                self.db.supersede_open_batches(subscription_id)
            self.db.insert_audit({
                "at": now, "actor": actor, "action": "subscription.update_rules",
                "target_type": "subscription", "target_id": subscription_id,
                "detail": {
                    "regions": new_regions, "fields": new_fields,
                    "region_changed": region_changed,
                },
            })
        return self.db.get_subscription(subscription_id)

    def pause_subscription(self, subscription_id: str, *, actor: str = "ops") -> None:
        self._set_sub_status(subscription_id, SUB_PAUSED, "subscription.pause", actor)

    def resume_subscription(self, subscription_id: str, *, actor: str = "ops") -> None:
        self._set_sub_status(subscription_id, "active", "subscription.resume", actor)

    def revoke_subscription(self, subscription_id: str, *, actor: str = "ops") -> None:
        """撤销授权：未确认批次全部作废，立即停止暴露任何字段。"""
        now = self.clock.now()
        sub = self.db.get_subscription(subscription_id)
        if sub is None:
            raise NotFoundError("订阅不存在")
        with self.db.transaction():
            self.db.update_subscription(subscription_id, status=SUB_REVOKED, updated_at=now)
            self.db.supersede_open_batches(subscription_id)
            self.db.insert_audit({
                "at": now, "actor": actor, "action": "subscription.revoke",
                "target_type": "subscription", "target_id": subscription_id, "detail": {},
            })

    def _set_sub_status(
        self, subscription_id: str, status: str, action: str, actor: str
    ) -> None:
        now = self.clock.now()
        sub = self.db.get_subscription(subscription_id)
        if sub is None:
            raise NotFoundError("订阅不存在")
        with self.db.transaction():
            self.db.update_subscription(subscription_id, status=status, updated_at=now)
            self.db.insert_audit({
                "at": now, "actor": actor, "action": action,
                "target_type": "subscription", "target_id": subscription_id,
                "detail": {"status": status},
            })

    def replay_subscription(
        self, subscription_id: str, from_version: int = 0, *, actor: str = "ops"
    ) -> dict[str, Any]:
        """运营回放：游标回退、作废未确认批次，从指定版本重新切包。

        计费按 (订阅, 事件) 幂等，回放重投不会重复计费。
        """
        sub = self.db.get_subscription(subscription_id)
        if sub is None:
            raise NotFoundError("订阅不存在")
        if from_version < 0 or from_version > self.db.max_version():
            raise ValidationError("回放版本超出当前版本范围")
        now = self.clock.now()
        with self.db.transaction():
            self.db.update_subscription(
                subscription_id, cursor_version=from_version,
                last_pull_at=None, updated_at=now,
            )
            self.db.supersede_open_batches(subscription_id)
            self.db.insert_audit({
                "at": now, "actor": actor, "action": "subscription.replay",
                "target_type": "subscription", "target_id": subscription_id,
                "detail": {"from_version": from_version},
            })
        return self.db.get_subscription(subscription_id)

    # ================= 事件摄入（版本化增量日志） =================

    def ingest_event(
        self,
        event_id: str,
        station_id: str,
        region: str,
        payload: dict[str, Any],
        *,
        occurred_at: float | None = None,
        priority: int = 0,
    ) -> dict[str, Any]:
        """站点事件入版本日志。event_id 幂等：重复摄入返回已有版本。"""
        if not event_id or not station_id or not region:
            raise ValidationError("event_id、station_id、region 均不能为空")
        if not isinstance(payload, dict):
            raise ValidationError("payload 必须为对象")
        now = self.clock.now()
        with self.db.transaction():
            existing = self.db.get_increment_by_event(event_id)
            if existing is not None:
                return {"version": existing["version"], "event_id": event_id, "duplicate": True}
            version = self.db.insert_increment({
                "event_id": event_id, "station_id": station_id, "region": region,
                "priority": int(priority), "payload": payload,
                "occurred_at": occurred_at if occurred_at is not None else now,
                "ingested_at": now,
            })
        return {"version": version, "event_id": event_id, "duplicate": False}

    # ================= 拉取（生成增量包 + 租约） =================

    def pull(self, partner_id: str, subscription_id: str) -> dict[str, Any]:
        """合作方拉取下一批增量。

        - 暂停/撤销的订阅不得拉取；
        - 已有在途批次：租约未到期则原样返回（幂等重拉），已过期则同批次
          顺序重投（attempt +1），保证慢消费者场景下不乱序、不丢批；
        - 刷新频率限制两次成功拉取间隔，但存在高优先级待发事件时突破限制，
          突发拥堵优先送达；
        - 载荷在交付时按订阅当前字段白名单投影。
        """
        sub = self.get_subscription(subscription_id, partner_id)
        self._ensure_pullable(sub)
        with self.db.transaction():
            # 事务内重取，避免与并发确认/撤销竞争。
            sub = self.db.get_subscription(subscription_id)
            self._ensure_pullable(sub)
            now = self.clock.now()

            open_batch = self.db.open_batch(subscription_id)
            if open_batch is not None:
                if (
                    open_batch["lease_expires_at"] is not None
                    and open_batch["lease_expires_at"] <= now
                ):
                    self.db.update_batch(
                        open_batch["id"], attempt=open_batch["attempt"] + 1,
                        leased_at=now, lease_expires_at=now + sub["lease_seconds"],
                    )
                    open_batch = self.db.get_batch(open_batch["id"])
                self.db.touch_pull(subscription_id, now)
                return self._render_batch(sub, open_batch, events_billed=0)

            if sub["last_pull_at"] is not None and sub["min_interval_seconds"] > 0:
                elapsed = now - sub["last_pull_at"]
                if elapsed < sub["min_interval_seconds"] and not self.db.has_urgent_pending(
                    sub["regions"], sub["cursor_version"], PRIORITY_HIGH
                ):
                    raise ConflictError(
                        f"刷新频率限制：请 {sub['min_interval_seconds'] - elapsed:.1f}s 后再试",
                        code="rate_limited",
                    )

            increments = self.db.pending_increments(
                sub["regions"], sub["cursor_version"], sub["max_batch_size"]
            )
            if not increments:
                self.db.touch_pull(subscription_id, now)
                return {
                    "batch_id": None, "events": [], "events_billed": 0,
                    "next_cursor": sub["cursor_version"], "has_more": False,
                    "lease_expires_at": None,
                }

            batch_id = self.ids.new_id("b")
            self.db.insert_batch({
                "id": batch_id, "subscription_id": subscription_id,
                "seq": self.db.next_seq(subscription_id),
                "version_from": sub["cursor_version"],
                "version_to": increments[-1]["version"],
                "status": BATCH_LEASED, "attempt": 1,
                "leased_at": now, "lease_expires_at": now + sub["lease_seconds"],
                "acked_at": None, "created_at": now,
            })
            self.db.insert_batch_items(
                batch_id, [(e["event_id"], e["version"]) for e in increments]
            )
            # 幂等计费：INSERT OR IGNORE，重复投递与回放均不重复计费。
            billed = 0
            for e in increments:
                if self.db.insert_billing_ignore({
                    "subscription_id": subscription_id, "event_id": e["event_id"],
                    "batch_id": batch_id, "units": 1, "billed_at": now,
                }):
                    billed += 1
            self.db.touch_pull(subscription_id, now)
            batch = self.db.get_batch(batch_id)
            return self._render_batch(sub, batch, events_billed=billed)

    @staticmethod
    def _ensure_pullable(sub: dict[str, Any]) -> None:
        if sub["status"] == SUB_REVOKED:
            raise ForbiddenError("授权已撤销")
        if sub["status"] == SUB_PAUSED:
            raise ForbiddenError("订阅已暂停")

    def _render_batch(
        self, sub: dict[str, Any], batch: dict[str, Any], *, events_billed: int
    ) -> dict[str, Any]:
        """按订阅当前字段白名单渲染批次（撤销/缩字段后重投立即生效）。"""
        items = self.db.batch_items(batch["id"])
        events = [{
            "event_id": item["event_id"],
            "version": item["version"],
            "station_id": item["station_id"],
            "region": item["region"],
            "priority": item["priority"],
            "occurred_at": item["occurred_at"],
            "data": project_fields(item["payload"], sub["fields"]),
        } for item in items]
        has_more = bool(self.db.pending_increments(sub["regions"], batch["version_to"], 1))
        return {
            "batch_id": batch["id"],
            "seq": batch["seq"],
            "attempt": batch["attempt"],
            "events": events,
            "events_billed": events_billed,
            "next_cursor": batch["version_to"],
            "version_from": batch["version_from"],
            "version_to": batch["version_to"],
            "has_more": has_more,
            "lease_expires_at": batch["lease_expires_at"],
        }

    # ================= 确认（推进游标） =================

    def ack(self, partner_id: str, subscription_id: str, batch_id: str) -> dict[str, Any]:
        """确认批次：推进游标。重复确认无副作用、不重复计费。"""
        self.get_subscription(subscription_id, partner_id)
        with self.db.transaction():
            sub = self.db.get_subscription(subscription_id)
            if sub["status"] == SUB_REVOKED:
                raise ForbiddenError("授权已撤销")
            batch = self.db.get_batch(batch_id)
            if batch is None or batch["subscription_id"] != subscription_id:
                raise NotFoundError("批次不存在")
            if batch["status"] == BATCH_ACKED:
                # 幂等确认：直接返回当前游标，不产生任何副作用。
                return {
                    "batch_id": batch_id, "cursor": sub["cursor_version"],
                    "duplicate": True,
                }
            if batch["status"] == BATCH_SUPERSEDED:
                raise ConflictError("批次已失效，请重新拉取", code="batch_superseded")
            if batch["version_from"] != sub["cursor_version"]:
                raise ConflictError(
                    "存在更早的未确认批次，必须按顺序确认", code="out_of_order"
                )
            now = self.clock.now()
            self.db.update_batch(batch_id, status=BATCH_ACKED, acked_at=now)
            self.db.update_subscription(subscription_id, cursor_version=batch["version_to"])
            self.db.insert_audit({
                "at": now, "actor": partner_id, "action": "batch.ack",
                "target_type": "batch", "target_id": batch_id,
                "detail": {"version_to": batch["version_to"], "attempt": batch["attempt"]},
            })
            return {"batch_id": batch_id, "cursor": batch["version_to"], "duplicate": False}

    # ================= 查询与审计 =================

    def _partner(self, partner_id: str) -> dict[str, Any]:
        partner = self.db.get_partner(partner_id)
        if partner is None:
            raise NotFoundError("合作方不存在")
        return partner

    def list_batches(self, subscription_id: str) -> list[dict[str, Any]]:
        return self.db.list_batches(subscription_id)

    def billing_report(self, subscription_id: str | None = None) -> list[dict[str, Any]]:
        return self.db.list_billing(subscription_id)

    def audit_trail(self, target_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return self.db.list_audit(target_id, limit)

    def cursor_of(self, subscription_id: str) -> int:
        sub = self.db.get_subscription(subscription_id)
        if sub is None:
            raise NotFoundError("订阅不存在")
        return int(sub["cursor_version"])

    def close(self) -> None:
        self.db.close()
