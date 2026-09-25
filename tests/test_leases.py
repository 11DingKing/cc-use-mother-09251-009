"""交付租约与慢消费者：租约有效期内幂等、超时回收、顺序重投。"""
from __future__ import annotations

from service_09251_009.domain import models as m
from service_09251_009.errors import ConflictError

from _base import ServiceTestBase


class LeaseTests(ServiceTestBase):
    lease_ttl = 10.0

    def test_idempotent_fetch_within_lease(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        self.emit("e2")
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        owner = r1.batch.lease_owner

        # 租约未过期，反复拉取返回同一批次同一租约令牌
        self.clock.advance(5)
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.batch_id, r1.batch.batch_id)
        self.assertEqual(r2.batch.lease_owner, owner)
        self.assertEqual(r2.batch.delivery_attempt, 1)
        self.assertEqual(self.store.billing_count(), 2)

        # 原令牌仍可确认
        cursor = self.service.ack_batch(
            pid, secret, sub.subscription_id, r1.batch.batch_id, owner
        )
        self.assertEqual(cursor.last_acked_seq, 2)

    def test_lease_expiry_redelivers_in_order(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        self.emit("e2")
        self.emit("e3")
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        stale_owner = r1.batch.lease_owner

        # 慢消费者超过租约 TTL 未确认
        self.clock.advance(self.lease_ttl + 1)
        expired = self.service.expire_leases()
        self.assertEqual(expired, [r1.batch.batch_id])

        # 旧令牌确认必须被拒绝
        with self.assertRaises(ConflictError):
            self.service.ack_batch(
                pid, secret, sub.subscription_id, r1.batch.batch_id, stale_owner
            )

        # 重新拉取：同一批次、同序事件、attempt +1
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.batch_id, r1.batch.batch_id)
        self.assertEqual(r2.batch.event_ids, ("e1", "e2", "e3"))
        self.assertEqual(r2.batch.delivery_attempt, 2)
        self.assertNotEqual(r2.batch.lease_owner, stale_owner)

        # 重投不重复计费
        self.assertEqual(self.store.billing_count(), 3)

        # 新持有者确认成功，游标推进
        cursor = self.service.ack_batch(
            pid, secret, sub.subscription_id, r2.batch.batch_id, r2.batch.lease_owner
        )
        self.assertEqual(cursor.last_acked_seq, 3)

        # 旧令牌在新租约下仍然无法确认
        with self.assertRaises(ConflictError):
            self.service.ack_batch(
                pid, secret, sub.subscription_id, r1.batch.batch_id, stale_owner
            )

    def test_slow_consumer_blocks_queue_until_ack_or_expiry(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.emit("e2")
        self.emit("e3")
        # 慢消费者占着未确认的批次，后续事件只能在队列后等
        for _ in range(3):
            self.clock.advance(1)
            r = self.service.fetch_batch(pid, secret, sub.subscription_id)
            self.assertEqual(r.batch.batch_id, r1.batch.batch_id)

        self.service.ack_batch(
            pid, secret, sub.subscription_id, r1.batch.batch_id, r1.batch.lease_owner
        )
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.event_ids, ("e2", "e3"))

    def test_redelivery_keeps_failed_batch_order(self) -> None:
        """失败重试不会跳过失败批次：未 ack 前拿不到后续事件。"""
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0, max_batch_size=2)
        for i in range(5):
            self.emit(f"e{i}")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("e0", "e1"))
        # 超时重投两轮，仍只能拿到这两条
        for attempt in range(2, 4):
            self.clock.advance(self.lease_ttl + 1)
            self.service.expire_leases()
            rr = self.service.fetch_batch(pid, secret, sub.subscription_id)
            self.assertEqual(rr.batch.event_ids, ("e0", "e1"))
            self.assertEqual(rr.batch.delivery_attempt, attempt)
        self.service.ack_batch(
            pid, secret, sub.subscription_id, r.batch.batch_id,
            self.store.get_batch(r.batch.batch_id).lease_owner,
        )
        nxt = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(nxt.batch.event_ids, ("e2", "e3"))
