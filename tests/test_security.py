"""授权与密钥：轮换宽限、吊销、停用，以及撤销后不再暴露历史敏感字段。"""
import tempfile
import unittest

from helpers import SAFE_FIELDS, ingest, make_service, make_subscription
from service_09251_009.domain import AuthError, ForbiddenError


class KeyRotationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.service, self.clock = make_service(f"{self._tmp.name}/t.db")

    def tearDown(self):
        self.service.close()
        self._tmp.cleanup()

    def test_rotation_grace_then_old_key_expires(self):
        """密钥轮换：宽限期内新旧都可用，宽限期后旧密钥失效。"""
        partner = self.service.register_partner("导航A")
        pid = partner["partner_id"]
        old_key = partner["api_key"]

        rotated = self.service.rotate_key(pid, grace_seconds=60.0)
        self.assertEqual(self.service.authenticate(old_key)["id"], pid)
        self.assertEqual(self.service.authenticate(rotated["api_key"])["id"], pid)

        self.clock.advance(61.0)
        with self.assertRaises(AuthError):
            self.service.authenticate(old_key)
        self.assertEqual(self.service.authenticate(rotated["api_key"])["id"], pid)

    def test_revoked_key_rejected_immediately(self):
        partner = self.service.register_partner("导航B")
        pid = partner["partner_id"]
        key_id = self.service.list_keys(pid)[0]["id"]

        self.service.revoke_key(key_id)
        with self.assertRaises(AuthError):
            self.service.authenticate(partner["api_key"])

    def test_suspended_partner_rejected_until_reactivated(self):
        partner = self.service.register_partner("导航C")
        pid = partner["partner_id"]
        self.service.suspend_partner(pid)
        with self.assertRaises(ForbiddenError):
            self.service.authenticate(partner["api_key"])
        self.service.reactivate_partner(pid)
        self.assertEqual(self.service.authenticate(partner["api_key"])["id"], pid)

    def test_unknown_key_rejected(self):
        with self.assertRaises(AuthError):
            self.service.authenticate("not-a-real-key")


class RevocationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.service, self.clock = make_service(f"{self._tmp.name}/t.db")
        self.partner, self.sub = make_subscription(self.service, lease=30.0)
        self.pid = self.partner["partner_id"]
        self.sid = self.sub["id"]

    def tearDown(self):
        self.service.close()
        self._tmp.cleanup()

    def test_field_reduction_applies_to_inflight_batch(self):
        """字段缩减立即生效：在途批次重投时不再暴露被移除的敏感字段。"""
        ingest(self.service, 1)
        batch = self.service.pull(self.pid, self.sid)
        self.assertIn("operator_notes", batch["events"][0]["data"])

        self.service.update_rules(self.sid, fields=SAFE_FIELDS)
        redelivered = self.service.pull(self.pid, self.sid)
        self.assertEqual(redelivered["batch_id"], batch["batch_id"])
        data = redelivered["events"][0]["data"]
        self.assertNotIn("operator_notes", data)
        self.assertIn("station_id", data)

    def test_revoked_subscription_blocks_pull_and_ack(self):
        """授权撤销后：拉取与确认都被拒绝，历史敏感字段不再暴露。"""
        ingest(self.service, 1)
        batch = self.service.pull(self.pid, self.sid)
        self.assertIn("operator_notes", batch["events"][0]["data"])

        self.service.revoke_subscription(self.sid)
        with self.assertRaises(ForbiddenError):
            self.service.pull(self.pid, self.sid)
        with self.assertRaises(ForbiddenError):
            self.service.ack(self.pid, self.sid, batch["batch_id"])

    def test_audit_trail_records_ops_actions(self):
        self.service.pause_subscription(self.sid)
        self.service.resume_subscription(self.sid)
        self.service.replay_subscription(self.sid, from_version=0)
        self.service.revoke_subscription(self.sid)
        actions = [a["action"] for a in self.service.audit_trail(self.sid)]
        self.assertEqual(
            actions,
            [
                "subscription.create",
                "subscription.pause",
                "subscription.resume",
                "subscription.replay",
                "subscription.revoke",
            ],
        )


if __name__ == "__main__":
    unittest.main()
