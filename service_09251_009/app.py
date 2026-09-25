"""应用装配：从数据库路径构建服务，供 CLI 与测试复用。"""
from __future__ import annotations

from pathlib import Path

from .ports.clock import (
    Clock,
    IdGenerator,
    RandomIdGenerator,
    SystemClock,
)
from .services.subscription_service import DEFAULT_LEASE_TTL, SubscriptionService
from .storage.sqlite_store import SQLiteStore


def build_service(
    db_path: str | Path = ":memory:",
    *,
    clock: Clock | None = None,
    id_gen: IdGenerator | None = None,
    lease_ttl: float = DEFAULT_LEASE_TTL,
) -> tuple[SubscriptionService, SQLiteStore]:
    """构建 (服务, 存储)。传入文件路径时游标与发件箱在重启后恢复。"""
    store = SQLiteStore(db_path)
    service = SubscriptionService(
        store,
        clock=clock or SystemClock(),
        id_gen=id_gen or RandomIdGenerator(),
        lease_ttl=lease_ttl,
    )
    return service, store
