"""持久化恢复：游标与发件箱在重启后恢复。"""
import os
import tempfile
import unittest

from helpers import event_ids, ingest, make_service, make_subscription


class RecoveryTests(unittest.TestCase):
    def test_cursor_and_outbox_survive_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "t.db")

            # 第一次运行：摄入 3 条并租出一个批次（未确认即在途发件箱）。
            s1, clock = make_service(db)
            partner, sub = make_subscription(s1, lease=10.0)
            pid, sid = partner["partner_id"], sub["id"]
            ingest(s1, 3)
            batch = s1.pull(pid, sid)
            self.assertEqual(len(batch["events"]), 3)
            s1.close()

            # 重启：在途批次从发件箱恢复，内容一致。
            s2, _ = make_service(db, clock=clock)
            restored = s2.pull(pid, sid)
            self.assertEqual(restored["batch_id"], batch["batch_id"])
            self.assertEqual(event_ids(restored), event_ids(batch))
            s2.ack(pid, sid, restored["batch_id"])
            s2.close()

            # 再重启：游标已推进，无重复投递。
            s3, _ = make_service(db, clock=clock)
            self.assertEqual(s3.cursor_of(sid), 3)
            self.assertEqual(s3.pull(pid, sid)["events"], [])
            s3.close()

    def test_expired_lease_released_after_restart(self):
        """重启后租约已过期的在途批次按原顺序重投，attempt 递增。"""
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "t.db")

            s1, clock = make_service(db)
            partner, sub = make_subscription(s1, lease=10.0)
            pid, sid = partner["partner_id"], sub["id"]
            ingest(s1, 2)
            batch = s1.pull(pid, sid)
            s1.close()

            clock.advance(11.0)  # 停机期间租约过期
            s2, _ = make_service(db, clock=clock)
            retry = s2.pull(pid, sid)
            self.assertEqual(retry["batch_id"], batch["batch_id"])
            self.assertEqual(retry["attempt"], 2)
            self.assertEqual(event_ids(retry), event_ids(batch))
            s2.close()

    def test_billing_and_audit_survive_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "t.db")

            s1, clock = make_service(db)
            partner, sub = make_subscription(s1)
            pid, sid = partner["partner_id"], sub["id"]
            ingest(s1, 2)
            batch = s1.pull(pid, sid)
            s1.ack(pid, sid, batch["batch_id"])
            s1.close()

            s2, _ = make_service(db, clock=clock)
            self.assertEqual(len(s2.billing_report(sid)), 2)
            actions = [a["action"] for a in s2.audit_trail()]
            self.assertIn("batch.ack", actions)
            s2.close()


if __name__ == "__main__":
    unittest.main()
