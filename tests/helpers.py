"""测试共享工具：确定性时钟/标识 + 常用搭建函数。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service_09251_009.ports import ManualClock, SequentialIds  # noqa: E402
from service_09251_009.service import SubscriptionService  # noqa: E402
from service_09251_009.storage import Storage  # noqa: E402

FULL_FIELDS = ["*"]
SAFE_FIELDS = ["station_id", "status", "available_chargers", "wait_minutes"]


def make_service(db_path: str, clock: ManualClock | None = None):
    clock = clock or ManualClock()
    service = SubscriptionService(Storage(db_path), clock=clock, ids=SequentialIds())
    return service, clock


def make_subscription(service, regions=("east",), fields=None, min_interval=0.0,
                      max_batch=50, lease=30.0, name="默认订阅"):
    partner = service.register_partner("测试出行平台")
    sub = service.create_subscription(
        partner["partner_id"], name,
        regions=list(regions),
        fields=list(fields) if fields is not None else None,
        min_interval_seconds=min_interval,
        max_batch_size=max_batch,
        lease_seconds=lease,
    )
    return partner, sub


def ingest(service, count, region="east", priority=0, prefix="ev", start=0):
    versions = []
    for i in range(start, start + count):
        result = service.ingest_event(
            event_id=f"{prefix}-{i}",
            station_id=f"st-{i}",
            region=region,
            priority=priority,
            payload={
                "station_id": f"st-{i}",
                "status": "busy",
                "available_chargers": i,
                "wait_minutes": 5,
                "operator_notes": "内部备注",
            },
        )
        versions.append(result["version"])
    return versions


def event_ids(batch):
    return [e["event_id"] for e in batch["events"]]
