"""可替换端口：时间、标识与密钥材料。

所有需要时间、随机标识或密钥材料的组件都依赖这里的协议，
测试可以注入确定性的实现，稳定复现状态变化。
"""
from __future__ import annotations

import secrets
import time
import uuid
from typing import Protocol


class Clock(Protocol):
    """时间来源。"""

    def now(self) -> float:
        """返回当前时间（Unix 秒）。"""
        ...


class SystemClock:
    """生产时钟。"""

    def now(self) -> float:
        return time.time()


class ManualClock:
    """测试用手动时钟，可确定性推进。"""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        self._now += float(seconds)
        return self._now


class IdGenerator(Protocol):
    """标识来源。"""

    def new_id(self, prefix: str) -> str:
        ...


class UuidIds:
    """生产标识生成器。"""

    def new_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:20]}"


class SequentialIds:
    """测试用确定性标识生成器。"""

    def __init__(self) -> None:
        self._n = 0

    def new_id(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}_{self._n:06d}"


class KeyProvider(Protocol):
    """API 密钥材料来源。"""

    def generate_key(self) -> str:
        ...


class SecretKeyProvider:
    """生产密钥生成器（加密安全随机）。"""

    def generate_key(self) -> str:
        return secrets.token_urlsafe(24)
