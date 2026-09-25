"""SQLite 重启恢复：游标、发件箱、租约、计费在进程重启后恢复。"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from service_09251_009.app import build_service
from service_09251_009.domain import models as m
from service_09251_009.ports.clock import FixedClock


class PersistenceRestartTests(unittest.TestCase):
    lease_ttl = 10.0

    def _build(self, db_path, clock):
        return build_service(db_path, clock=clock, lease_ttl=self.lease_ttl)

    def test_cursor_outbox_and_billing_survive_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "state.db"
            clock = FixedClock(1_000_000.0)
            svc, store = self._build(db, clock)

            svc.register_partner("p1", "甲")
            kid, secret = svc.issue_api_key("p1")
            sub = svc.create_subscription("p1", refresh_interval=0)
            for i in range(4):
                svc.ingest_event(
                    event_id=f"e{i}", station_id=f"s{i}", area="east",
                    status=m.StationStatus.AVAILABLE,
                    payload={"operator_note": f"n{i}"},
                )
            r = svc.fetch_batch("p1", secret, sub.subscription_id)
            svc.ack_batch(
                "p1", secret, sub.subscription_id,
                r.batch.batch_id, r.batch.lease_owner,
            )
            svc.ingest_event(
                event_id="e4", station_id="s4", area="east",
                status=m.StationStatus.CONGESTED,
                congestion=m.CongestionLevel.HIGH, payload={},
            )
            r2 = svc.fetch_batch("p1", secret, sub.subscription_id)
            self.assertEqual(r2.batch.event_ids, ("e4",))
            store.close()

            # 重启：新服务实例指向同一数据库文件
            clock2 = FixedClock(clock.now() + 1)
            svc2, store2 = self._build(db, clock2)
            partner = store2.get_partner("p1")
            self.assertIsNotNone(partner)
            self.assertTrue(partner.authenticate(secret, clock2.now()))
            self.assertEqual(store2.get_cursor(sub.subscription_id).last_acked_seq, 4)

            # 未确认批次仍在发件箱中且租约仍有效
            live = store2.outstanding_batch(sub.subscription_id)
            self.assertIsNotNone(live)
            self.assertEqual(live.batch_id, r2.batch.batch_id)
            self.assertEqual(live.event_ids, ("e4",))

            # 用原租约令牌确认（TTL 未过）
            cursor = svc2.ack_batch(
                "p1", secret, sub.subscription_id, live.batch_id, live.lease_owner
            )
            self.assertEqual(cursor.last_acked_seq, 5)

            # 计费记录恢复，不重复
            self.assertEqual(store2.billing_count(), 5)
            self.assertIsNone(svc2.fetch_batch("p1", secret, sub.subscription_id).batch)
            store2.close()

    def test_expired_lease_recovered_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "state.db"
            clock = FixedClock(1_000_000.0)
            svc, store = self._build(db, clock)
            svc.register_partner("p1", "甲")
            _kid, secret = svc.issue_api_key("p1")
            sub = svc.create_subscription("p1", refresh_interval=0)
            svc.ingest_event(
                event_id="e1", station_id="s1", area="east",
                status=m.StationStatus.AVAILABLE, payload={},
            )
            r = svc.fetch_batch("p1", secret, sub.subscription_id)
            store.close()

            # 重启时已超过租约 TTL
            clock2 = FixedClock(clock.now() + self.lease_ttl + 5)
            svc2, store2 = self._build(db, clock2)
            expired = svc2.expire_leases()
            self.assertEqual(expired, [r.batch.batch_id])
            r2 = svc2.fetch_batch("p1", secret, sub.subscription_id)
            self.assertEqual(r2.batch.batch_id, r.batch.batch_id)
            self.assertEqual(r2.batch.delivery_attempt, 2)
            self.assertEqual(self.store_billing(store2), 1)
            store2.close()

    @staticmethod
    def store_billing(store) -> int:
        return store.billing_count()
