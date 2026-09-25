"""HTTP/JSON 接口端到端：合作方拉取/确认，运营方管理/回放/审计。"""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

from service_09251_009.api.http_api import create_server
from service_09251_009.app import build_service
from service_09251_009.domain import models as m
from service_09251_009.ports.clock import FixedClock, SequentialIdGenerator

from _base import ServiceTestBase


OP_TOKEN = "op-secret-token"


class HttpApiTests(ServiceTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.httpd, self.api = create_server(self.service, OP_TOKEN, port=0)
        self.port = self.httpd.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        super().tearDown()

    def _request(self, method: str, path: str, body=None, token: str | None = None,
                 partner_id: str | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self.base}{path}", data=data, method=method,
        )
        req.add_header("Content-Type", "application/json")
        if token is not None:
            req.add_header("Authorization", f"Bearer {token}")
        if partner_id is not None:
            req.add_header("X-Partner-Id", partner_id)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_full_lifecycle_over_http(self) -> None:
        # 运营注册合作方并创建订阅
        st, body = self._request("POST", "/v1/operator/partners",
                                 {"partner_id": "p1", "name": "出行App"}, token=OP_TOKEN)
        self.assertEqual(st, 200)
        secret = body["initial_secret"]

        st, body = self._request(
            "POST", "/v1/operator/subscriptions",
            {"partner_id": "p1", "refresh_interval": 0, "areas": ["east"]},
            token=OP_TOKEN,
        )
        self.assertEqual(st, 200)
        sub_id = body["subscription_id"]

        # 摄入两条事件，一条区域不匹配
        for i, area in enumerate(("east", "west")):
            st, body = self._request(
                "POST", "/v1/internal/events",
                {"event_id": f"e{i}", "station_id": f"s{i}", "area": area,
                 "payload": {"operator_note": f"n{i}"}},
                token=OP_TOKEN,
            )
            self.assertEqual(st, 200)
            self.assertTrue(body["ingested"])

        # 无鉴权拉取被拒
        st, body = self._request(
            "GET", f"/v1/subscriptions/{sub_id}/batch", token=None, partner_id="p1"
        )
        self.assertEqual(st, 401)

        # 正常拉取
        st, body = self._request(
            "GET", f"/v1/subscriptions/{sub_id}/batch",
            token=secret, partner_id="p1",
        )
        self.assertEqual(st, 200)
        wire = body["batch"]
        self.assertEqual([e["event_id"] for e in wire["events"]], ["e0"])
        self.assertEqual(wire["state"], "delivered")

        # 确认
        st, body = self._request(
            "POST", f"/v1/subscriptions/{sub_id}/ack",
            {"batch_id": wire["batch_id"], "lease_owner": wire["lease_owner"]},
            token=secret, partner_id="p1",
        )
        self.assertEqual(st, 200)
        self.assertEqual(body["last_acked_seq"], 1)

        # 区域调整 + 回放 + 审计
        st, _ = self._request(
            "POST", f"/v1/operator/subscriptions/{sub_id}/rules",
            {"areas": ["west"]}, token=OP_TOKEN,
        )
        self.assertEqual(st, 200)
        st, _ = self._request(
            "POST", f"/v1/operator/subscriptions/{sub_id}/replay",
            {"from_seq": 0}, token=OP_TOKEN,
        )
        self.assertEqual(st, 200)
        st, body = self._request(
            "GET", "/v1/operator/audit", token=OP_TOKEN,
        )
        self.assertEqual(st, 200)
        actions = {e["action"] for e in body["entries"]}
        self.assertIn("rule_adjusted", actions)
        self.assertIn("batch_replayed", actions)

    def test_revoked_secret_rejected_over_http(self) -> None:
        st, body = self._request("POST", "/v1/operator/partners",
                                 {"partner_id": "p9", "name": "临时平台"},
                                 token=OP_TOKEN)
        self.assertEqual(st, 200)
        secret = body["initial_secret"]
        st, body = self._request(
            "POST", "/v1/operator/subscriptions",
            {"partner_id": "p9", "refresh_interval": 0}, token=OP_TOKEN,
        )
        sub_id = body["subscription_id"]
        self._request("POST", "/v1/operator/partners/p9/revoke",
                      {"reason": "合规"}, token=OP_TOKEN)
        st, body = self._request(
            "GET", f"/v1/subscriptions/{sub_id}/batch",
            token=secret, partner_id="p9",
        )
        self.assertEqual(st, 401)
        self.assertEqual(body["error"], "AuthorizationError")

    def test_paused_returns_423(self) -> None:
        self.service.register_partner("p2", "乙")
        _k, secret = self.service.issue_api_key("p2")
        sub = self.service.create_subscription("p2", refresh_interval=0)
        self.service.suspend_partner("p2")
        st, body = self._request(
            "GET", f"/v1/subscriptions/{sub.subscription_id}/batch",
            token=secret, partner_id="p2",
        )
        self.assertEqual(st, 423)
        self.assertEqual(body["error"], "SubscriptionPausedError")

    def test_queue_shows_priority_ordering(self) -> None:
        st, _ = self._request("POST", "/v1/operator/partners",
                              {"partner_id": "pa", "name": "A"}, token=OP_TOKEN)
        self.assertEqual(st, 200)
        st, body = self._request("POST", "/v1/operator/subscriptions",
                                 {"partner_id": "pa", "refresh_interval": 0},
                                 token=OP_TOKEN)
        sub_a = body["subscription_id"]
        self.emit("x", station_id="sx")
        self.service.materialize_due()
        st, body = self._request("GET", "/v1/operator/queue", token=OP_TOKEN)
        self.assertEqual(st, 200)
        self.assertTrue(any(q["subscription_id"] == sub_a for q in body["queue"]))

    def test_dispatch_endpoint_pops_priority_batch(self) -> None:
        st, body = self._request("POST", "/v1/operator/partners",
                                 {"partner_id": "pa", "name": "A"}, token=OP_TOKEN)
        self.assertEqual(st, 200)
        st, body = self._request("POST", "/v1/operator/subscriptions",
                                 {"partner_id": "pa", "refresh_interval": 0},
                                 token=OP_TOKEN)
        sub_id = body["subscription_id"]
        self.emit("z", station_id="sz",
                  congestion=m.CongestionLevel.CRITICAL)
        self.service.materialize_due()
        st, body = self._request("POST", "/v1/operator/dispatch", {}, token=OP_TOKEN)
        self.assertEqual(st, 200)
        self.assertTrue(body["dispatched"])
        self.assertEqual(body["batch"]["subscription_id"], sub_id)
        self.assertEqual(body["batch"]["state"], "delivered")
        st, body = self._request("POST", "/v1/operator/dispatch", {}, token=OP_TOKEN)
        self.assertFalse(body["dispatched"])

    def test_malformed_json_returns_400(self) -> None:
        req = urllib.request.Request(
            f"{self.base}/v1/operator/partners",
            data=b"{not-json", method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {OP_TOKEN}")
        try:
            urllib.request.urlopen(req, timeout=5)
            self.fail("应当返回 400")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)
            self.assertEqual(json.loads(e.read())["error"], "ValidationError")

    def test_operator_token_required(self) -> None:
        st, _ = self._request("GET", "/v1/operator/queue")
        self.assertEqual(st, 401)
