"""领域错误。"""
from __future__ import annotations


class SubscriptionError(Exception):
    """所有订阅服务错误的基类。"""


class NotFoundError(SubscriptionError):
    """合作方或订阅不存在。"""


class AuthorizationError(SubscriptionError):
    """密钥无效或授权已撤销。"""


class SubscriptionPausedError(SubscriptionError):
    """运营方已暂停该合作方的订阅交付。"""


class ConflictError(SubscriptionError):
    """并发冲突：租约所有者不符或状态已变化。"""


class RateLimitedError(SubscriptionError):
    """拉取频率高于订阅约定的刷新节奏。"""


class ValidationError(SubscriptionError):
    """输入不合法。"""
