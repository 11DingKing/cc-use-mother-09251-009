"""跨平台充电状态订阅的服务端包入口。"""
from __future__ import annotations

PROJECT_CODE = "service_09251_009"

__all__ = ["PROJECT_CODE", "project_info", "SubscriptionService", "Storage"]


def project_info() -> dict[str, str]:
    """返回稳定的项目标识。"""
    return {"code": PROJECT_CODE, "title": "跨平台充电状态订阅"}


def __getattr__(name: str):
    # 惰性导出，保持包导入轻量。
    if name == "SubscriptionService":
        from .service import SubscriptionService
        return SubscriptionService
    if name == "Storage":
        from .storage import Storage
        return Storage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
