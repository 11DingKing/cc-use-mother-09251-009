"""端口：时钟与标识生成器，可替换以便测试稳定复现。"""
from __future__ import annotations

import itertools
import secrets
import time


class Clock:
    """可替换时钟端口。"""

    def now(self) -> float:
        raise NotImplementedError


class SystemClock(Clock):
    def now(self) -> float:
        return time.time()


class FixedClock(Clock):
    """测试时钟：手动推进。"""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> float:
        self._t += seconds
        return self._t

    def set(self, value: float) -> None:
        self._t = value


class IdGenerator:
    """稳定标识生成端口。"""

    def new_id(self, prefix: str) -> str:
        raise NotImplementedError


class SequentialIdGenerator(IdGenerator):
    """进程内顺序 ID（测试用，可预测）。"""

    def __init__(self) -> None:
        self._counter = itertools.count(1)

    def new_id(self, prefix: str) -> str:
        return f"{prefix}_{next(self._counter)}"


class RandomIdGenerator(IdGenerator):
    """随机 ID（默认实现）：重启后不会与历史主键冲突。"""

    def new_id(self, prefix: str) -> str:
        return f"{prefix}_{secrets.token_hex(8)}"
