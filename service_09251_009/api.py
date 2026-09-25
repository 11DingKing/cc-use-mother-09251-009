"""HTTP 接口边界：合作方拉取/确认批次，运营方管理订阅与审计。

仅使用标准库。认证方式：
- 合作方接口：``X-Api-Key: <key>`` 或 ``Authorization: Bearer <key>``
- 运营接口：``X-Admin-Token: <token>``

错误统一为 ``{"error": {"code": ..., "message": ...}}``。
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .domain import AuthError, ServiceError, ValidationError
from .service import SubscriptionService


def make_handler(service: SubscriptionService, admin_token: str):
    """构造绑定到应用服务的请求处理器。"""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        # ---- 基础工具 ----

        def log_message(self, *args: Any) -> None:  # 静默访问日志
            return

        def _send(self, status: int, obj: dict[str, Any]) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise ValidationError("请求体不是合法 JSON")
            if not isinstance(data, dict):
                raise ValidationError("请求体必须是 JSON 对象")
            return data

        def _require_admin(self) -> None:
            token = self.headers.get("X-Admin-Token", "")
            if not admin_token or token != admin_token:
                raise AuthError("运营方令牌无效")

        def _partner_id(self) -> str:
            api_key = self.headers.get("X-Api-Key", "")
            if not api_key:
                auth = self.headers.get("Authorization", "")
                if auth.startswith("Bearer "):
                    api_key = auth[len("Bearer "):]
            return service.authenticate(api_key)["id"]

        # ---- 分发 ----

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

        def _dispatch(self, method: str) -> None:
            try:
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                body = self._read_json() if method == "POST" else {}
                status, obj = self._route(method, parsed.path, query, body)
                self._send(status, obj)
            except ServiceError as exc:
                self._send(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
            except Exception as exc:  # noqa: BLE001 - 边界兜底，不外泄堆栈
                self._send(500, {"error": {"code": "internal_error", "message": str(exc)}})

        def _route(
            self, method: str, path: str,
            query: dict[str, list[str]], body: dict[str, Any],
        ) -> tuple[int, dict[str, Any]]:
            m: re.Match[str] | None

            if method == "GET" and path == "/health":
                return 200, {"status": "up"}

            # ---------- 合作方接口 ----------
            if method == "POST" and path == "/v1/pull":
                partner_id = self._partner_id()
                result = service.pull(partner_id, _str(body, "subscription_id"))
                return 200, result

            if method == "POST" and path == "/v1/ack":
                partner_id = self._partner_id()
                result = service.ack(
                    partner_id, _str(body, "subscription_id"), _str(body, "batch_id")
                )
                return 200, result

            # ---------- 运营接口 ----------
            if method == "POST" and path == "/admin/partners":
                self._require_admin()
                return 201, service.register_partner(_str(body, "name"))

            if (m := re.fullmatch(r"/admin/partners/([^/]+)/keys/rotate", path)) and method == "POST":
                self._require_admin()
                return 201, service.rotate_key(
                    m.group(1),
                    grace_seconds=_float(body, "grace_seconds", 3600.0),
                    label=str(body.get("label") or "rotated"),
                )

            if (m := re.fullmatch(r"/admin/partners/([^/]+)/keys", path)) and method == "GET":
                self._require_admin()
                return 200, {"items": service.list_keys(m.group(1))}

            if (m := re.fullmatch(r"/admin/keys/([^/]+)/revoke", path)) and method == "POST":
                self._require_admin()
                service.revoke_key(m.group(1))
                return 200, {"revoked": m.group(1)}

            if (m := re.fullmatch(r"/admin/partners/([^/]+)/suspend", path)) and method == "POST":
                self._require_admin()
                service.suspend_partner(m.group(1))
                return 200, {"partner_id": m.group(1), "status": "suspended"}

            if (m := re.fullmatch(r"/admin/partners/([^/]+)/reactivate", path)) and method == "POST":
                self._require_admin()
                service.reactivate_partner(m.group(1))
                return 200, {"partner_id": m.group(1), "status": "active"}

            if method == "POST" and path == "/admin/subscriptions":
                self._require_admin()
                sub = service.create_subscription(
                    _str(body, "partner_id"), _str(body, "name"),
                    regions=_list(body, "regions"),
                    fields=_list(body, "fields", required=False),
                    min_interval_seconds=_float(body, "min_interval_seconds", 0.0),
                    max_batch_size=_int(body, "max_batch_size", 100),
                    lease_seconds=_float(body, "lease_seconds", 300.0),
                )
                return 201, sub

            if (m := re.fullmatch(r"/admin/subscriptions/([^/]+)/rules", path)) and method == "POST":
                self._require_admin()
                sub = service.update_rules(
                    m.group(1),
                    regions=_list(body, "regions", required=False),
                    fields=_list(body, "fields", required=False),
                    min_interval_seconds=_opt_float(body, "min_interval_seconds"),
                    max_batch_size=_opt_int(body, "max_batch_size"),
                    lease_seconds=_opt_float(body, "lease_seconds"),
                )
                return 200, sub

            if (m := re.fullmatch(r"/admin/subscriptions/([^/]+)/(pause|resume|revoke)", path)) and method == "POST":
                self._require_admin()
                sub_id, action = m.group(1), m.group(2)
                if action == "pause":
                    service.pause_subscription(sub_id)
                elif action == "resume":
                    service.resume_subscription(sub_id)
                else:
                    service.revoke_subscription(sub_id)
                return 200, {"subscription_id": sub_id, "action": action}

            if (m := re.fullmatch(r"/admin/subscriptions/([^/]+)/replay", path)) and method == "POST":
                self._require_admin()
                sub = service.replay_subscription(
                    m.group(1), from_version=_int(body, "from_version", 0)
                )
                return 200, sub

            if method == "POST" and path == "/admin/events":
                self._require_admin()
                result = service.ingest_event(
                    _str(body, "event_id"), _str(body, "station_id"), _str(body, "region"),
                    body.get("payload") if "payload" in body else {},
                    occurred_at=_opt_float(body, "occurred_at"),
                    priority=_int(body, "priority", 0),
                )
                return (200 if result["duplicate"] else 201), result

            if method == "GET" and path == "/admin/audit":
                self._require_admin()
                target = query.get("target_id", [None])[0]
                return 200, {"items": service.audit_trail(target)}

            if method == "GET" and path == "/admin/billing":
                self._require_admin()
                sub_id = query.get("subscription_id", [None])[0]
                items = service.billing_report(sub_id)
                return 200, {"items": items, "total_units": sum(i["units"] for i in items)}

            if (m := re.fullmatch(r"/admin/subscriptions/([^/]+)/batches", path)) and method == "GET":
                self._require_admin()
                return 200, {"items": service.list_batches(m.group(1))}

            return 404, {"error": {"code": "not_found", "message": "接口不存在"}}

    return Handler


def _str(body: dict[str, Any], key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"缺少必填字段：{key}")
    return value


def _list(body: dict[str, Any], key: str, required: bool = True) -> list[str] | None:
    value = body.get(key)
    if value is None:
        if required:
            raise ValidationError(f"缺少必填字段：{key}")
        return None
    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
        raise ValidationError(f"字段 {key} 必须是字符串数组")
    return value


def _int(body: dict[str, Any], key: str, default: int) -> int:
    value = body.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"字段 {key} 必须是数字")
    return int(value)


def _opt_int(body: dict[str, Any], key: str) -> int | None:
    return None if body.get(key) is None else _int(body, key, 0)


def _float(body: dict[str, Any], key: str, default: float) -> float:
    value = body.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"字段 {key} 必须是数字")
    return float(value)


def _opt_float(body: dict[str, Any], key: str) -> float | None:
    return None if body.get(key) is None else _float(body, key, 0.0)


def make_server(
    service: SubscriptionService,
    admin_token: str,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ThreadingHTTPServer:
    """创建（尚未启动的）HTTP 服务器。port=0 时由系统分配端口。"""
    server = ThreadingHTTPServer((host, port), make_handler(service, admin_token))
    server.daemon_threads = True
    return server
