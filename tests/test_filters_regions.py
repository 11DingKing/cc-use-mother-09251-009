"""过滤规则：区域过滤与调整、刷新频率限制、突发拥堵优先送达。"""
import tempfile
import unittest

from helpers import event_ids, ingest, make_service, make_subscription
from service_09251_009.domain import PRIORITY_HIGH, ConflictError


class RegionFilterTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.service, self.clock = make_service(f"{self._tmp.name}/t.db")

    def tearDown(self):
        self.service.close()
        self._tmp.cleanup()

    def test_region_filtering(self):
        """订阅只收到白名单区域内的事件。"""
        partner, sub = make_subscription(self.service, regions=("east",))
        pid, sid = partner["partner_id"], sub["id"]
        ingest(self.service, 2, region="east", prefix="e")
        ingest(self.service, 1, region="west", prefix="w")

        batch = self.service.pull(pid, sid)
        self.assertEqual(event_ids(batch), ["e-0", "e-1"])
        self.service.ack(pid, sid, batch["batch_id"])
        self.assertEqual(self.service.pull(pid, sid)["events"], [])

    def test_region_adjustment_widens_and_narrows(self):
        """区域调整：扩大后补投游标之后的新匹配事件，收窄后不再投递被移除区域。"""
        partner, sub = make_subscription(self.service, regions=("east",))
        pid, sid = partner["partner_id"], sub["id"]
        ingest(self.service, 1, region="east", prefix="e")
        ingest(self.service, 1, region="west", prefix="w")

        batch = self.service.pull(pid, sid)
        self.assertEqual(event_ids(batch), ["e-0"])
        self.service.ack(pid, sid, batch["batch_id"])

        # 扩大区域：west 事件版本在游标之后，被补投。
        self.service.update_rules(sid, regions=["east", "west"])
        widened = self.service.pull(pid, sid)
        self.assertEqual(event_ids(widened), ["w-0"])
        self.service.ack(pid, sid, widened["batch_id"])

        # 收窄区域：新的 west 事件不再投递。
        self.service.update_rules(sid, regions=["east"])
        ingest(self.service, 1, region="west", prefix="w", start=1)
        ingest(self.service, 1, region="east", prefix="e", start=1)
        narrowed = self.service.pull(pid, sid)
        self.assertEqual(event_ids(narrowed), ["e-1"])

    def test_region_change_supersedes_unacked_batch(self):
        """区域调整作废旧批次：旧批次确认被拒，新批次按新区域切包。"""
        partner, sub = make_subscription(self.service, regions=("east", "west"))
        pid, sid = partner["partner_id"], sub["id"]
        ingest(self.service, 1, region="east", prefix="e")
        ingest(self.service, 1, region="west", prefix="w")

        stale = self.service.pull(pid, sid)
        self.assertEqual(event_ids(stale), ["e-0", "w-0"])

        self.service.update_rules(sid, regions=["east"])
        fresh = self.service.pull(pid, sid)
        self.assertEqual(event_ids(fresh), ["e-0"])
        self.assertNotEqual(fresh["batch_id"], stale["batch_id"])

        with self.assertRaises(ConflictError) as ctx:
            self.service.ack(pid, sid, stale["batch_id"])
        self.assertEqual(ctx.exception.code, "batch_superseded")

        self.service.ack(pid, sid, fresh["batch_id"])
        self.assertEqual(self.service.cursor_of(sid), 1)


class RefreshAndPriorityTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.service, self.clock = make_service(f"{self._tmp.name}/t.db")
        self.partner, self.sub = make_subscription(self.service, min_interval=60.0)
        self.pid = self.partner["partner_id"]
        self.sid = self.sub["id"]

    def tearDown(self):
        self.service.close()
        self._tmp.cleanup()

    def test_refresh_interval_throttles_pulls(self):
        """刷新频率：未到间隔的拉取被限流，间隔过后恢复。"""
        ingest(self.service, 1, prefix="a")
        first = self.service.pull(self.pid, self.sid)
        self.assertEqual(len(first["events"]), 1)
        self.service.ack(self.pid, self.sid, first["batch_id"])

        ingest(self.service, 1, prefix="a", start=1)
        with self.assertRaises(ConflictError) as ctx:
            self.service.pull(self.pid, self.sid)
        self.assertEqual(ctx.exception.code, "rate_limited")

        self.clock.advance(61.0)
        second = self.service.pull(self.pid, self.sid)
        self.assertEqual(event_ids(second), ["a-1"])

    def test_urgent_events_bypass_throttle(self):
        """突发拥堵：高优先级事件突破刷新频率限制，优先送达。"""
        ingest(self.service, 1, prefix="n")
        first = self.service.pull(self.pid, self.sid)
        self.service.ack(self.pid, self.sid, first["batch_id"])

        ingest(self.service, 1, prefix="urgent", priority=PRIORITY_HIGH)
        urgent = self.service.pull(self.pid, self.sid)  # 间隔未到仍立即送达
        self.assertEqual(event_ids(urgent), ["urgent-0"])
        self.assertEqual(urgent["events"][0]["priority"], PRIORITY_HIGH)


if __name__ == "__main__":
    unittest.main()
