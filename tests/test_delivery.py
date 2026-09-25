"""基础交付路径：增量包、游标推进、顺序保证、幂等计费/确认。"""
from __future__ import annotations

from service_09251_009.domain import models as m
from service_09251_009.errors import ConflictError, RateLimitedError

from _base import ServiceTestBase


class BasicDeliveryTests(ServiceTestBase):
    def test_incremental_batches_and_cursor(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)

        self.emit("e1")
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r1.batch.event_ids, ("e1",))
        self.assertEqual(r1.batch.rule_version, 1)
        self.assertEqual(r1.batch.delivery_attempt, 1)

        self.clock.advance(1)
        self.emit("e2")
        self.emit("e3")
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        # 未确认前不产生新批次，幂等返回同一批次
        self.assertEqual(r2.batch.batch_id, r1.batch.batch_id)

        cursor = self.service.ack_batch(
            pid, secret, sub.subscription_id, r1.batch.batch_id, r1.batch.lease_owner
        )
        self.assertEqual(cursor.last_acked_seq, 1)

        r3 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r3.batch.event_ids, ("e2", "e3"))
        seqs = [ev["seq"] for ev in r3.batch.to_wire()["events"]]
        self.assertEqual(seqs, sorted(seqs))  # 顺序不被重排

        self.service.ack_batch(
            pid, secret, sub.subscription_id, r3.batch.batch_id, r3.batch.lease_owner
        )
        r4 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertIsNone(r4.batch)
        self.assertEqual(self.store.billing_count(), 3)

    def test_duplicate_event_id_is_idempotent(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("dup")
        again = self.service.ingest_event(
            event_id="dup",
            station_id="other",
            area="east",
            status=m.StationStatus.FAULT,
            payload={},
        )
        self.assertIsNone(again)
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("dup",))

    def test_duplicate_ack_is_idempotent(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        owner = r.batch.lease_owner
        self.service.ack_batch(pid, secret, sub.subscription_id, r.batch.batch_id, owner)
        # 再确认一次：幂等成功，无新增计费、无额外审计
        audits_before = len(self.service.audit_trail(action=m.AuditAction.BATCH_ACKED))
        cursor = self.service.ack_batch(
            pid, secret, sub.subscription_id, r.batch.batch_id, owner
        )
        self.assertEqual(cursor.last_acked_seq, 1)
        audits_after = len(self.service.audit_trail(action=m.AuditAction.BATCH_ACKED))
        self.assertEqual(audits_before, audits_after)
        self.assertEqual(self.store.billing_count(), 1)

    def test_ack_with_wrong_owner_rejected(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        with self.assertRaises(ConflictError):
            self.service.ack_batch(
                pid, secret, sub.subscription_id, r.batch.batch_id, "impostor:deadbeef"
            )

    def test_max_batch_size_limits_package(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0, max_batch_size=2)
        for i in range(5):
            self.emit(f"e{i}")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("e0", "e1"))
        self.service.ack_batch(
            pid, secret, sub.subscription_id, r.batch.batch_id, r.batch.lease_owner
        )
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.event_ids, ("e2", "e3"))

    def test_refresh_interval_is_enforced_for_new_pulls(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=10)
        self.emit("e1")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.service.ack_batch(
            pid, secret, sub.subscription_id, r.batch.batch_id, r.batch.lease_owner
        )
        self.emit("e2")
        # 未到刷新节奏：新批次的拉取被限流
        with self.assertRaises(RateLimitedError):
            self.service.fetch_batch(pid, secret, sub.subscription_id)
        # 但租约内的幂等重试不受限（重试路径不检查节奏）
        self.clock.advance(9)
        with self.assertRaises(RateLimitedError):
            self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.clock.advance(2)
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.event_ids, ("e2",))
