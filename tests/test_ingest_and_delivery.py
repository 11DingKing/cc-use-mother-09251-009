"""摄入幂等、顺序交付、慢消费者租约重投、幂等确认/计费、回放与暂停。"""
import tempfile
import unittest

from helpers import event_ids, ingest, make_service, make_subscription
from service_09251_009.domain import ForbiddenError


class IngestTests(unittest.TestCase):
    def test_duplicate_event_ingested_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = make_service(f"{tmp}/t.db")
            first = service.ingest_event("e1", "s1", "east", {"a": 1})
            second = service.ingest_event("e1", "s1", "east", {"a": 1})
            self.assertFalse(first["duplicate"])
            self.assertTrue(second["duplicate"])
            self.assertEqual(first["version"], second["version"])
            service.close()


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.service, self.clock = make_service(f"{self._tmp.name}/t.db")
        self.partner, self.sub = make_subscription(
            self.service, max_batch=2, lease=10.0
        )
        self.pid = self.partner["partner_id"]
        self.sid = self.sub["id"]

    def tearDown(self):
        self.service.close()
        self._tmp.cleanup()

    def test_batches_in_order_and_cursor_advances_on_ack(self):
        ingest(self.service, 3)
        b1 = self.service.pull(self.pid, self.sid)
        self.assertEqual(event_ids(b1), ["ev-0", "ev-1"])
        self.assertEqual(b1["seq"], 1)
        ack1 = self.service.ack(self.pid, self.sid, b1["batch_id"])
        self.assertEqual(ack1["cursor"], 2)

        b2 = self.service.pull(self.pid, self.sid)
        self.assertEqual(event_ids(b2), ["ev-2"])
        self.service.ack(self.pid, self.sid, b2["batch_id"])
        self.assertEqual(self.service.cursor_of(self.sid), 3)

        empty = self.service.pull(self.pid, self.sid)
        self.assertEqual(empty["events"], [])
        self.assertIsNone(empty["batch_id"])

    def test_slow_consumer_lease_expiry_redelivers_same_batch_in_order(self):
        """慢消费者：租约过期未确认 → 同批次同顺序重投，attempt 递增。"""
        ingest(self.service, 2)
        first = self.service.pull(self.pid, self.sid)
        self.assertEqual(first["attempt"], 1)

        self.clock.advance(11.0)  # 超过 10s 租约
        retry = self.service.pull(self.pid, self.sid)
        self.assertEqual(retry["batch_id"], first["batch_id"])
        self.assertEqual(retry["attempt"], 2)
        self.assertEqual(event_ids(retry), event_ids(first))

        ack = self.service.ack(self.pid, self.sid, retry["batch_id"])
        self.assertEqual(ack["cursor"], 2)
        self.assertEqual(len(self.service.billing_report(self.sid)), 2)

    def test_pull_while_lease_held_returns_same_batch(self):
        """租约未到期时重拉是幂等的：返回同一批次，不生成新批次。"""
        ingest(self.service, 2)
        first = self.service.pull(self.pid, self.sid)
        again = self.service.pull(self.pid, self.sid)
        self.assertEqual(again["batch_id"], first["batch_id"])
        self.assertEqual(again["attempt"], 1)
        self.assertEqual(len(self.service.list_batches(self.sid)), 1)

    def test_duplicate_ack_is_idempotent_and_not_double_billed(self):
        """相同事件不能重复确认、重复计费。"""
        ingest(self.service, 2)
        batch = self.service.pull(self.pid, self.sid)
        self.assertEqual(batch["events_billed"], 2)

        first = self.service.ack(self.pid, self.sid, batch["batch_id"])
        second = self.service.ack(self.pid, self.sid, batch["batch_id"])
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["cursor"], second["cursor"])
        self.assertEqual(len(self.service.billing_report(self.sid)), 2)

    def test_replay_redelivers_without_double_billing(self):
        """运营回放：游标回退后重投历史事件，但不重复计费。"""
        ingest(self.service, 2)
        batch = self.service.pull(self.pid, self.sid)
        self.service.ack(self.pid, self.sid, batch["batch_id"])

        self.service.replay_subscription(self.sid, from_version=0)
        replayed = self.service.pull(self.pid, self.sid)
        self.assertEqual(event_ids(replayed), ["ev-0", "ev-1"])
        self.assertEqual(replayed["events_billed"], 0)
        self.service.ack(self.pid, self.sid, replayed["batch_id"])
        self.assertEqual(len(self.service.billing_report(self.sid)), 2)
        self.assertEqual(self.service.cursor_of(self.sid), 2)

    def test_pause_blocks_pull_until_resumed(self):
        ingest(self.service, 1)
        self.service.pause_subscription(self.sid)
        with self.assertRaises(ForbiddenError):
            self.service.pull(self.pid, self.sid)
        self.service.resume_subscription(self.sid)
        self.assertEqual(len(self.service.pull(self.pid, self.sid)["events"]), 1)


if __name__ == "__main__":
    unittest.main()
