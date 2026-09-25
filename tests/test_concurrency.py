"""并发确认与并发拉取：单活批次、原子确认、计费不重复。"""
from __future__ import annotations

import threading

from service_09251_009.domain import models as m
from service_09251_009.errors import ConflictError

from _base import ServiceTestBase


class ConcurrencyTests(ServiceTestBase):
    def test_concurrent_ack_only_one_commit_across_lease_generations(self) -> None:
        """租约换代后旧/新属主并发确认：新属主恰好提交一次，旧属主全部冲突；
        新属主的重复确认幂等成功；计费与审计都只有一份。"""
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        for i in range(10):
            self.emit(f"e{i}")
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        stale_owner = r1.batch.lease_owner

        # 租约超时后重投，产生新属主
        self.clock.advance(self.lease_ttl + 1)
        self.service.expire_leases()
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        current_owner = r2.batch.lease_owner
        self.assertNotEqual(stale_owner, current_owner)

        outcomes: list[str] = []
        barrier = threading.Barrier(8)
        lock = threading.Lock()

        def confirm(owner: str) -> None:
            barrier.wait()
            try:
                self.service.ack_batch(
                    pid, secret, sub.subscription_id, r2.batch.batch_id, owner
                )
                result = "ok"
            except ConflictError:
                result = "conflict"
            with lock:
                outcomes.append(result)

        threads = (
            [threading.Thread(target=confirm, args=(stale_owner,)) for _ in range(4)]
            + [threading.Thread(target=confirm, args=(current_owner,)) for _ in range(4)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 旧属主全部被拒；新属主恰好一次提交，其余幂等成功
        self.assertEqual(outcomes.count("conflict"), 4)
        self.assertEqual(outcomes.count("ok"), 4)
        self.assertEqual(
            len(self.service.audit_trail(action=m.AuditAction.BATCH_ACKED)), 1
        )
        self.assertEqual(self.store.get_cursor(sub.subscription_id).last_acked_seq, 10)
        self.assertEqual(self.store.billing_count(), 10)
        self.assertEqual(
            self.store.get_batch(r2.batch.batch_id).state, m.LeaseState.ACKED
        )

    def test_concurrent_fetch_same_subscription_single_batch(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        for i in range(6):
            self.emit(f"e{i}")

        batches: list[str] = []
        errors: list[Exception] = []
        barrier = threading.Barrier(6)
        lock = threading.Lock()

        def pull() -> None:
            barrier.wait()
            try:
                result = self.service.fetch_batch(pid, secret, sub.subscription_id)
                with lock:
                    batches.append(result.batch.batch_id)
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=pull) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertFalse(errors, f"并发拉取出现异常: {errors}")
        self.assertEqual(len(batches), 6)
        self.assertEqual(set(batches), {batches[0]})  # 全部拿到同一批次
        self.assertEqual(self.store.billing_count(), 6)  # 计费按指纹去重

    def test_concurrent_fetch_and_ack_race(self) -> None:
        """一个线程确认，多个线程拉取：最终只剩新的未确认包或空，状态永不重复。"""
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0, max_batch_size=2)
        for i in range(4):
            self.emit(f"e{i}")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)

        outcome: list[str] = []
        barrier = threading.Barrier(3)
        lock = threading.Lock()

        def ack_old() -> None:
            barrier.wait()
            try:
                self.service.ack_batch(
                    pid, secret, sub.subscription_id, r.batch.batch_id,
                    r.batch.lease_owner,
                )
                with lock:
                    outcome.append("acked")
            except ConflictError:
                with lock:
                    outcome.append("conflict")

        def pull() -> None:
            barrier.wait()
            result = self.service.fetch_batch(pid, secret, sub.subscription_id)
            with lock:
                outcome.append(
                    result.batch.batch_id if result.batch is not None else "empty"
                )

        threads = [
            threading.Thread(target=ack_old),
            threading.Thread(target=pull),
            threading.Thread(target=pull),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 合法交错有两种：拉取都发生在确认前（仅第一批 2 个指纹），
        # 或确认后有拉取（再建第二批，4 个指纹）。关键是没有重复计费、
        # 没有重复批次、游标只可能是 0 或 2。
        fingerprints = {b["fingerprint"] for b in self.store.list_billing()}
        self.assertIn(len(fingerprints), (2, 4))
        self.assertIn(
            self.store.get_cursor(sub.subscription_id).last_acked_seq, {0, 2}
        )
        live = [
            b for b in self.store.list_batches(sub.subscription_id)
            if b.state in (m.LeaseState.PENDING, m.LeaseState.DELIVERED, m.LeaseState.EXPIRED)
        ]
        self.assertLessEqual(len(live), 1)
