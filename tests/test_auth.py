"""合作方授权：密钥轮换、暂停/恢复、撤销后停止暴露并脱敏历史敏感字段。"""
from __future__ import annotations

from service_09251_009.domain import models as m
from service_09251_009.errors import AuthorizationError, SubscriptionPausedError

from _base import ServiceTestBase


class AuthTests(ServiceTestBase):
    def test_bad_secret_rejected(self) -> None:
        pid, _kid, _secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        with self.assertRaises(AuthorizationError):
            self.service.fetch_batch(pid, "wrong-secret", sub.subscription_id)

    def test_key_rotation_old_key_fails_new_key_works(self) -> None:
        pid, _kid, old_secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")

        rotated = self.service.rotate_api_key(pid)
        new_secret = rotated["new_secret"]
        self.assertNotEqual(old_secret, new_secret)

        # 旧密钥立即失效
        with self.assertRaises(AuthorizationError):
            self.service.fetch_batch(pid, old_secret, sub.subscription_id)
        # 新密钥可用
        r = self.service.fetch_batch(pid, new_secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("e1",))

    def test_expired_key_rejected(self) -> None:
        pid, original_kid, _original_secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        # 新密钥 5 秒后过期，并立刻作废旧密钥
        _kid, soon_secret = self.service.issue_api_key(
            pid, expires_at=self.clock.now() + 5
        )
        self.service.revoke_api_key(pid, original_kid)
        self.clock.advance(6)
        self.emit("e1")
        with self.assertRaises(AuthorizationError):
            self.service.fetch_batch(pid, soon_secret, sub.subscription_id)

    def test_suspend_blocks_then_resume(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.service.suspend_partner(pid, reason="对账")
        with self.assertRaises(SubscriptionPausedError):
            self.service.fetch_batch(pid, secret, sub.subscription_id)
        # 暂停期间确认同样被拒绝
        with self.assertRaises(SubscriptionPausedError):
            self.service.ack_batch(
                pid, secret, sub.subscription_id, r.batch.batch_id, r.batch.lease_owner
            )
        self.service.resume_partner(pid)
        # 恢复后原批次仍在，租约未失（暂停不回收），可继续确认
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.batch_id, r.batch.batch_id)
        cursor = self.service.ack_batch(
            pid, secret, sub.subscription_id, r2.batch.batch_id, r2.batch.lease_owner
        )
        self.assertEqual(cursor.last_acked_seq, 1)

    def test_revocation_stops_delivery_and_redacts_history(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(
            pid, refresh_interval=0,
            fields=m.KNOWN_FIELDS,
        )
        self.emit("e1", payload={"internal_code": "X-1", "repair_contact": "110"})
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        wire = r1.batch.to_wire()["events"][0]
        self.assertEqual(wire["internal_code"], "X-1")
        self.service.ack_batch(
            pid, secret, sub.subscription_id, r1.batch.batch_id, r1.batch.lease_owner
        )

        # 再来一批未确认的
        self.emit("e2", payload={"internal_code": "X-2", "repair_contact": "120"})
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.to_wire()["events"][0]["internal_code"], "X-2")

        # 撤销授权
        self.service.revoke_partner(pid, reason="违规")

        # 密钥不再有效
        with self.assertRaises(AuthorizationError):
            self.service.fetch_batch(pid, secret, sub.subscription_id)

        # 未确认批次状态变为 revoked，退出投递队列
        self.assertEqual(
            self.store.get_batch(r2.batch.batch_id).state, m.LeaseState.REVOKED
        )
        self.assertEqual(
            [b.batch_id for b in self.service.delivery_queue()
             if b.subscription_id == sub.subscription_id],
            []
        )

        # 历史批次（含已确认）敏感字段一律脱敏，非敏感字段保留
        for batch_id in (r1.batch.batch_id, r2.batch.batch_id):
            for ev in self.store.get_batch(batch_id).to_wire()["events"]:
                self.assertIsNone(ev["internal_code"])
                self.assertIsNone(ev["repair_contact"])
                self.assertIsNone(ev["operator_note"])
                self.assertEqual(ev["station_id"], self.store.get_batch(batch_id)
                                 .items[0].event.station_id)

    def test_revoked_partner_cannot_resume_via_suspend_path(self) -> None:
        pid, _k, _s = self.given_partner_with_key()
        self.service.revoke_partner(pid)
        from service_09251_009.errors import ValidationError
        with self.assertRaises(ValidationError):
            self.service.resume_partner(pid)

    def test_cannot_access_other_partners_subscription(self) -> None:
        p1, _k1, s1 = self.given_partner_with_key("p1", "甲")
        self.service.register_partner("p2", "乙")
        _k2, s2 = self.service.issue_api_key("p2")
        sub1 = self.service.create_subscription(p1, refresh_interval=0)
        self.emit("e1")
        with self.assertRaises(AuthorizationError):
            self.service.fetch_batch("p2", s2, sub1.subscription_id)
