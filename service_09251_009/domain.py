"""领域模型：常量、错误与字段投影规则。"""
from __future__ import annotations

import hashlib
from typing import Any

# 事件优先级：突发拥堵等紧急事件使用高优先级，可突破刷新频率限制优先送达。
PRIORITY_NORMAL = 0
PRIORITY_HIGH = 10

# 字段白名单通配符。
ALL_FIELDS = "*"

# 订阅状态。
SUB_ACTIVE = "active"
SUB_PAUSED = "paused"
SUB_REVOKED = "revoked"

# 批次（发件箱）状态。
BATCH_LEASED = "leased"        # 已租出，等待确认
BATCH_ACKED = "acked"          # 已确认
BATCH_SUPERSEDED = "superseded"  # 被回放取代，需重新拉取

# 合作方密钥状态。
KEY_ACTIVE = "active"
KEY_RETIRING = "retiring"  # 轮换宽限期内的旧密钥
KEY_REVOKED = "revoked"

# 合作方状态。
PARTNER_ACTIVE = "active"
PARTNER_SUSPENDED = "suspended"


class ServiceError(Exception):
    """应用层错误，携带稳定的错误码与 HTTP 状态。"""

    status = 500
    code = "internal_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code:
            self.code = code


class ValidationError(ServiceError):
    status = 400
    code = "validation_error"


class AuthError(ServiceError):
    status = 401
    code = "auth_failed"


class ForbiddenError(ServiceError):
    status = 403
    code = "forbidden"


class NotFoundError(ServiceError):
    status = 404
    code = "not_found"


class ConflictError(ServiceError):
    status = 409
    code = "conflict"


def hash_key(api_key: str) -> str:
    """密钥只存散列，不落明文。"""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def project_fields(payload: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    """按当前授权的字段白名单投影载荷。

    渲染发生在交付时刻，因此字段缩减或授权撤销立即生效，
    历史批次重投时也不会再暴露被移除的敏感字段。
    """
    if ALL_FIELDS in fields:
        return dict(payload)
    return {k: v for k, v in payload.items() if k in fields}
