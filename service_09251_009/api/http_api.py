"""HTTP/JSON 接口边界。

合作方接口（API 密钥鉴权）：
- GET  /v1/subscriptions/{id}/batch        拉取增量包
- POST /v1/subscriptions/{id}/ack          确认批次

运营方接口（运营令牌鉴权）：
- POST /v1/operator/partners               注册合作方
- POST /v1/operator/partners/{id}/keys     签发密钥
- POST /v1/operator/partners/{id}/rotate   密钥轮换
- POST /v1/operator/partners/{id}/revoke   撤销授权
- POST /v1/operator/partners/{id}/suspend  暂停
- POST /v1/operator/partners/{id}/resume   恢复
- POST /v1/operator/subscriptions          创建订阅
- POST /v1/operator/subscriptions/{id}/rules  调整过滤规则（版本化）
- POST /v1/operator/subscriptions/{id}/replay  回放
- GET  /v1/operator/audit                  审计查询
- GET  /v1/operator/queue                  待交付队列
- GET  /v1/operator/cursors/{id}           查看游标

内部接口（运营令牌）：
- POST /v1/internal/events                 站点事件摄入
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from ..domain import models as m
from ..errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    RateLimitedError,
    SubscriptionPausedError,
    ValidationError,
)
from ..services.subscription_service import SubscriptionService


def _congestion(value: str | None) -> m.CongestionLevel:
    if value is None:
        return m.CongestionLevel.NORMAL
    return m.CongestionLevel(value)


def _status(value: str) -> m.StationStatus:
    return m.StationStatus(value)


class SubscriptionApi:
    """无状态控制器：把 JSON 字典映射到应用服务，返回可序列化结果。"""

    def __init__(self, service: SubscriptionService, operator_token: str) -> None:
        self.service = service
        self.operator_token = operator_token

    # -- 合作方侧 -----------------------------------------------------------

    def fetch(self, partner_id: str, secret: str, sub_id: str, query: dict[str, str]):
        max_items = int(query["max_items"]) if query.get("max_items") else None
        result = self.service.fetch_batch(
            partner_id, secret, sub_id, max_items=max_items
        )
        if result.batch is None:
            return {"batch": None, "polled": True}
        return {"batch": result.batch.to_wire(), "polled": True}

    def ack(self, partner_id: str, secret: str, sub_id: str, body: dict[str, Any]):
        cursor = self.service.ack_batch(
            partner_id,
            secret,
            sub_id,
            body["batch_id"],
            body["lease_owner"],
        )
        return {
            "subscription_id": cursor.subscription_id,
            "last_acked_seq": cursor.last_acked_seq,
            "updated_at": cursor.updated_at,
        }

    # -- 运营侧 -------------------------------------------------------------

    def register_partner(self, body: dict[str, Any]) -> dict[str, Any]:
        partner = self.service.register_partner(body["partner_id"], body["name"])
        key_id, secret = self.service.issue_api_key(partner.partner_id)
        return {
            "partner_id": partner.partner_id,
            "name": partner.name,
            "initial_key_id": key_id,
            "initial_secret": secret,
        }

    def issue_key(self, partner_id: str, body: dict[str, Any]) -> dict[str, Any]:
        key_id, secret = self.service.issue_api_key(
            partner_id, expires_at=body.get("expires_at")
        )
        return {"key_id": key_id, "secret": secret}

    def rotate_key(self, partner_id: str, body: dict[str, Any]) -> dict[str, Any]:
        return self.service.rotate_api_key(
            partner_id, expires_at=body.get("expires_at")
        )

    def revoke_partner(self, partner_id: str, body: dict[str, Any]) -> dict[str, Any]:
        self.service.revoke_partner(partner_id, body.get("reason", ""))
        return {"partner_id": partner_id, "state": m.AuthState.REVOKED.value}

    def suspend_partner(self, partner_id: str, body: dict[str, Any]) -> dict[str, Any]:
        self.service.suspend_partner(partner_id, body.get("reason", ""))
        return {"partner_id": partner_id, "state": m.AuthState.SUSPENDED.value}

    def resume_partner(self, partner_id: str) -> dict[str, Any]:
        self.service.resume_partner(partner_id)
        return {"partner_id": partner_id, "state": m.AuthState.ACTIVE.value}

    def create_subscription(self, body: dict[str, Any]) -> dict[str, Any]:
        sub = self.service.create_subscription(
            body["partner_id"],
            refresh_interval=float(body.get("refresh_interval", 5.0)),
            max_batch_size=int(body.get("max_batch_size", 100)),
            areas=body.get("areas"),
            fields=body.get("fields"),
            min_congestion=_congestion(body.get("min_congestion")),
            station_ids=body.get("station_ids"),
        )
        return {
            "subscription_id": sub.subscription_id,
            "partner_id": sub.partner_id,
            "refresh_interval": sub.refresh_interval,
            "max_batch_size": sub.max_batch_size,
            "rule_version": sub.current_rule.version,
        }

    def adjust_rule(self, sub_id: str, body: dict[str, Any]) -> dict[str, Any]:
        rule = self.service.adjust_rule(
            sub_id,
            areas=body.get("areas", m.KEEP),
            fields=body.get("fields", m.KEEP),
            min_congestion=_congestion(body["min_congestion"])
            if "min_congestion" in body
            else m.KEEP,
            station_ids=body.get("station_ids", m.KEEP),
            reason=body.get("reason", ""),
        )
        return {
            "subscription_id": sub_id,
            "version": rule.version,
            "areas": sorted(rule.areas) if rule.areas else None,
            "fields": sorted(rule.fields) if rule.fields else None,
            "min_congestion": rule.min_congestion.value,
            "changed_at": rule.changed_at,
        }

    def replay(self, sub_id: str, body: dict[str, Any]) -> dict[str, Any]:
        self.service.replay(
            sub_id,
            from_seq=int(body.get("from_seq", 0)),
            actor=body.get("actor", "operator"),
        )
        return {"subscription_id": sub_id, "replayed": True}

    def audit(self, query: dict[str, str]) -> dict[str, Any]:
        action = m.AuditAction(query["action"]) if query.get("action") else None
        entries = self.service.audit_trail(
            subscription_id=query.get("subscription_id"),
            partner_id=query.get("partner_id"),
            action=action,
            limit=int(query["limit"]) if query.get("limit") else None,
        )
        return {"entries": entries}

    def queue(self) -> dict[str, Any]:
        return {
            "queue": [
                {
                    "batch_id": b.batch_id,
                    "subscription_id": b.subscription_id,
                    "priority": b.priority,
                    "state": b.state.value,
                    "events": len(b.items),
                }
                for b in self.service.delivery_queue()
            ]
        }

    def dispatch(self) -> dict[str, Any]:
        batch = self.service.dispatch_next()
        if batch is None:
            return {"dispatched": False, "batch": None}
        return {"dispatched": True, "batch": batch.to_wire()}

    def cursor(self, sub_id: str) -> dict[str, Any]:
        c = self.service.store.get_cursor(sub_id)
        return {
            "subscription_id": c.subscription_id,
            "last_acked_seq": c.last_acked_seq,
            "updated_at": c.updated_at,
        }

    # -- 内部 ---------------------------------------------------------------

    def ingest_event(self, body: dict[str, Any]) -> dict[str, Any]:
        event = self.service.ingest_event(
            event_id=body["event_id"],
            station_id=body["station_id"],
            area=body["area"],
            status=_status(body.get("status", m.StationStatus.AVAILABLE.value)),
            payload=body.get("payload", {}),
            congestion=_congestion(body.get("congestion")),
            occurred_at=body.get("occurred_at"),
        )
        if event is None:
            return {"ingested": False, "reason": "duplicate_event_id"}
        return {"ingested": True, "seq": event.seq, "event_id": event.event_id}


# ---------------------------------------------------------------------------
# HTTP 适配
# ---------------------------------------------------------------------------

_ERROR_STATUS = {
    AuthorizationError: 401,
    SubscriptionPausedError: 423,
    NotFoundError: 404,
    ConflictError: 409,
    RateLimitedError: 429,
    ValidationError: 400,
}


class _Handler(BaseHTTPRequestHandler):
    api: SubscriptionApi = None  # type: ignore[assignment]
    server_version = "ChargeSub/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # 静默
        return

    # -- 鉴权 ---------------------------------------------------------------

    def _auth_context(self) -> tuple[str, str]:
        partner = self.headers.get("X-Partner-Id", "")
        secret = self.headers.get("Authorization", "")
        if secret.startswith("Bearer "):
            secret = secret[len("Bearer ") :]
        return partner, secret

    def _require_operator(self) -> None:
        _, token = self._auth_context()
        if not token or token != self.api.operator_token:
            raise AuthorizationError("需要运营令牌")

    # -- 读写 ---------------------------------------------------------------

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            body = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValidationError(f"请求体不是合法 JSON: {exc}") from exc
        if not isinstance(body, dict):
            raise ValidationError("请求体必须是 JSON 对象")
        return body

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _query(self) -> dict[str, str]:
        q = parse_qs(urlparse(self.path).query)
        return {k: v[0] for k, v in q.items()}

    def _handle(self, fn: Callable[[], dict[str, Any]]) -> None:
        try:
            self._send_json(200, fn())
        except Exception as exc:  # noqa: BLE001 - 边界统一映射
            status = 500
            for etype, code in _ERROR_STATUS.items():
                if isinstance(exc, etype):
                    status = code
                    break
            else:
                if isinstance(exc, KeyError):
                    status = 400
                    exc = ValidationError(f"缺少必填字段: {exc.args[0]}")
                elif isinstance(exc, ValueError):
                    status = 400
                    exc = ValidationError(str(exc))
            self._send_json(
                status, {"error": type(exc).__name__, "message": str(exc)}
            )

    # -- 路由 ---------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        q = self._query()

        def route() -> dict[str, Any]:
            if path.startswith("/v1/subscriptions/") and path.endswith("/batch"):
                pid, secret = self._auth_context()
                sub_id = path.split("/")[3]
                return self.api.fetch(pid, secret, sub_id, q)
            if path == "/v1/operator/audit":
                self._require_operator()
                return self.api.audit(q)
            if path == "/v1/operator/queue":
                self._require_operator()
                return self.api.queue()
            if path == "/v1/operator/dispatch":
                self._require_operator()
                return self.api.dispatch()
            if path.startswith("/v1/operator/cursors/"):
                self._require_operator()
                return self.api.cursor(path.rsplit("/", 1)[1])
            raise NotFoundError(f"未知路径: {path}")

        self._handle(route)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"

        def route() -> dict[str, Any]:
            body = self._read_json()
            if path.startswith("/v1/subscriptions/") and path.endswith("/ack"):
                pid, secret = self._auth_context()
                sub_id = path.split("/")[3]
                return self.api.ack(pid, secret, sub_id, body)
            if path == "/v1/internal/events":
                self._require_operator()
                return self.api.ingest_event(body)
            if path == "/v1/operator/partners":
                self._require_operator()
                return self.api.register_partner(body)
            if path == "/v1/operator/dispatch":
                self._require_operator()
                return self.api.dispatch()
            if path.startswith("/v1/operator/partners/"):
                self._require_operator()
                tail = path[len("/v1/operator/partners/") :].split("/")
                pid = tail[0]
                action = tail[1] if len(tail) > 1 else ""
                if action == "keys":
                    return self.api.issue_key(pid, body)
                if action == "rotate":
                    return self.api.rotate_key(pid, body)
                if action == "revoke":
                    return self.api.revoke_partner(pid, body)
                if action == "suspend":
                    return self.api.suspend_partner(pid, body)
                if action == "resume":
                    return self.api.resume_partner(pid)
                raise NotFoundError(f"未知操作: {action}")
            if path == "/v1/operator/subscriptions":
                self._require_operator()
                return self.api.create_subscription(body)
            if path.startswith("/v1/operator/subscriptions/"):
                self._require_operator()
                tail = path[len("/v1/operator/subscriptions/") :].split("/")
                sub_id = tail[0]
                action = tail[1] if len(tail) > 1 else ""
                if action == "rules":
                    return self.api.adjust_rule(sub_id, body)
                if action == "replay":
                    return self.api.replay(sub_id, body)
                raise NotFoundError(f"未知操作: {action}")
            raise NotFoundError(f"未知路径: {path}")

        self._handle(route)


def create_server(
    service: SubscriptionService,
    operator_token: str,
    host: str = "127.0.0.1",
    port: int = 0,
) -> tuple[ThreadingHTTPServer, SubscriptionApi]:
    """创建可测试的 HTTP 服务器（port=0 时由系统分配端口）。"""
    api = SubscriptionApi(service, operator_token)

    class _BoundHandler(_Handler):
        pass

    _BoundHandler.api = api
    httpd = ThreadingHTTPServer((host, port), _BoundHandler)
    return httpd, api


def serve_in_thread(
    service: SubscriptionService,
    operator_token: str,
    host: str = "127.0.0.1",
    port: int = 0,
) -> tuple[ThreadingHTTPServer, threading.Thread, SubscriptionApi]:
    httpd, api = create_server(service, operator_token, host, port)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, api
