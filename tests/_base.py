"""测试公共夹具。"""
from __future__ import annotations

import unittest

from service_09251_009.app import build_service
from service_09251_009.ports.clock import FixedClock, SequentialIdGenerator
from service_09251_009.domain import models as m


class ServiceTestBase(unittest.TestCase):
    lease_ttl = 30.0

    def setUp(self) -> None:
        self.clock = FixedClock(1_000_000.0)
        self.ids = SequentialIdGenerator()
        self.service, self.store = build_service(
            ":memory:",
            clock=self.clock,
            id_gen=self.ids,
            lease_ttl=self.lease_ttl,
        )

    def given_partner_with_key(self, pid: str = "p1", name: str = "导航平台甲"):
        self.service.register_partner(pid, name)
        key_id, secret = self.service.issue_api_key(pid)
        return pid, key_id, secret

    def emit(
        self,
        event_id: str,
        *,
        station_id: str | None = None,
        area: str = "east",
        status: m.StationStatus = m.StationStatus.AVAILABLE,
        payload=None,
        congestion: m.CongestionLevel = m.CongestionLevel.NORMAL,
    ) -> m.StationEvent:
        event = self.service.ingest_event(
            event_id=event_id,
            station_id=station_id or f"st_{event_id}",
            area=area,
            status=status,
            payload={"operator_note": f"note_{event_id}", **(payload or {})},
            congestion=congestion,
        )
        assert event is not None
        return event
