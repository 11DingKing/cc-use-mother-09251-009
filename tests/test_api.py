"""HTTP 接口边界：合作方拉取/确认、运营管理与审计的端到端流程。"""
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from helpers import make_service
from service_09251_009.api import make_server

ADMIN = {"X-Admin-Token": "test-admin-token"}


class ApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.service, self.clock = make_service(f"{self._tmp.name}/t.db")
        self.server = make_server(self.service, "test-admin-token", "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.service.close()
        self._tmp.cleanup()

    def _request(self, method, path, body=None, headers=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        for key, value in (headers or {}).items():
            req.add_header(key, value)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def _setup_subscription(self):
        status, partner = self._request("POST", "/admin/partners", {"name": "导航A"}, ADMIN)
        self.assertEqual(status, 201)
        status, sub = self._request("POST", "/admin/subscriptions", {
            "partner_id": partner["partner_id"], "name": "默认",
            "regions": ["east"], "fields": ["station_id", "status"],
        }, ADMIN)
        self.assertEqual(status, 201)
        return partner, sub

    def test_full_pull_ack_flow(self):
        partner, sub = self._setup_subscription()
        key = {"X-Api-Key": partner["api_key"]}
        sid = sub["id"]

        for i in range(2):
            status, result = self._request("POST", "/admin/events", {
                "event_id": f"e{i}", "station_id": "st-1", "region": "east",
                "payload": {"station_id": "st-1", "status": "busy", "operator_notes": "x"},
            }, ADMIN)
            self.assertEqual(status, 201)
        # 重复摄入：幂等返回 200 + duplicate
        status, dup = self._request("POST", "/admin/events", {
            "event_id": "e0", "station_id": "st-1", "region": "east", "payload": {},
        }, ADMIN)
        self.assertEqual(status, 200)
        self.assertTrue(dup["duplicate"])

        status, batch = self._request("POST", "/v1/pull", {"subscription_id": sid}, key)
        self.assertEqual(status, 200)
        self.assertEqual(len(batch["events"]), 2)
        # 字段白名单生效：敏感字段不下发
        self.assertNotIn("operator_notes", batch["events"][0]["data"])

        status, ack = self._request(
            "POST", "/v1/ack",
            {"subscription_id": sid, "batch_id": batch["batch_id"]}, key)
        self.assertEqual(status, 200)
        self.assertFalse(ack["duplicate"])

        # 重复确认幂等
        status, ack2 = self._request(
            "POST", "/v1/ack",
            {"subscription_id": sid, "batch_id": batch["batch_id"]}, key)
        self.assertEqual(status, 200)
        self.assertTrue(ack2["duplicate"])

        status, billing = self._request("GET", f"/admin/billing?subscription_id={sid}", None, ADMIN)
        self.assertEqual(status, 200)
        self.assertEqual(billing["total_units"], 2)

        status, audit = self._request("GET", "/admin/audit", None, ADMIN)
        actions = {item["action"] for item in audit["items"]}
        self.assertIn("subscription.create", actions)
        self.assertIn("batch.ack", actions)

    def test_partner_auth_required(self):
        status, body = self._request(
            "POST", "/v1/pull", {"subscription_id": "s_x"}, {"X-Api-Key": "bad-key"})
        self.assertEqual(status, 401)
        self.assertEqual(body["error"]["code"], "auth_failed")

    def test_admin_token_required(self):
        status, body = self._request("POST", "/admin/partners", {"name": "x"})
        self.assertEqual(status, 401)
        self.assertEqual(body["error"]["code"], "auth_failed")

    def test_paused_subscription_rejects_pull(self):
        partner, sub = self._setup_subscription()
        key = {"X-Api-Key": partner["api_key"]}
        sid = sub["id"]
        self._request("POST", f"/admin/subscriptions/{sid}/pause", {}, ADMIN)
        status, body = self._request("POST", "/v1/pull", {"subscription_id": sid}, key)
        self.assertEqual(status, 403)

    def test_other_partners_subscription_not_visible(self):
        partner, sub = self._setup_subscription()
        status, other = self._request("POST", "/admin/partners", {"name": "导航B"}, ADMIN)
        status, body = self._request(
            "POST", "/v1/pull",
            {"subscription_id": sub["id"]}, {"X-Api-Key": other["api_key"]})
        self.assertEqual(status, 404)

    def test_key_rotation_over_http(self):
        partner, sub = self._setup_subscription()
        pid = partner["partner_id"]
        status, rotated = self._request(
            "POST", f"/admin/partners/{pid}/keys/rotate", {"grace_seconds": 3600}, ADMIN)
        self.assertEqual(status, 201)
        status, _ = self._request(
            "POST", "/v1/pull", {"subscription_id": sub["id"]},
            {"X-Api-Key": rotated["api_key"]})
        self.assertEqual(status, 200)


if __name__ == "__main__":
    unittest.main()
