"""过滤规则：区域、字段、拥堵等级、白名单、版本化调整。"""
from __future__ import annotations

from service_09251_009.domain import models as m
from service_09251_009.errors import ValidationError

from _base import ServiceTestBase


class FilterRuleTests(ServiceTestBase):
    def test_area_filter(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0, areas={"east"})
        self.emit("a", area="east")
        self.emit("b", area="west")
        self.emit("c", area="north")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("a",))

    def test_field_projection(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(
            pid,
            refresh_interval=0,
            fields={"station_id", "status", "available_connectors"},
        )
        self.emit("a", payload={"available_connectors": 2, "queue_length": 9})
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        event = r.batch.to_wire()["events"][0]
        self.assertIn("available_connectors", event)
        self.assertNotIn("queue_length", event)
        self.assertNotIn("operator_note", event)
        # 身份字段始终保留
        self.assertEqual(event["event_id"], "a")

    def test_unknown_field_rejected(self) -> None:
        pid, _k, _s = self.given_partner_with_key()
        with self.assertRaises(ValidationError):
            self.service.create_subscription(pid, fields={"not_a_field"})

    def test_min_congestion_filter(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(
            pid, refresh_interval=0, min_congestion=m.CongestionLevel.HIGH
        )
        self.emit("n1", congestion=m.CongestionLevel.NORMAL)
        self.emit("h1", area="east", congestion=m.CongestionLevel.HIGH)
        self.emit("n2", congestion=m.CongestionLevel.NORMAL)
        self.emit("c1", congestion=m.CongestionLevel.CRITICAL)
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("h1", "c1"))

    def test_station_whitelist(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(
            pid, refresh_interval=0, station_ids={"keep"}
        )
        self.emit("a", station_id="keep")
        self.emit("b", station_id="drop")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("a",))

    def test_area_adjustment_is_versioned_and_keep_semantics(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(
            pid, refresh_interval=0, areas={"east"},
            fields={"station_id", "status", "operator_note"},
        )
        self.emit("e1", area="east")
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r1.batch.rule_version, 1)
        self.service.ack_batch(
            pid, secret, sub.subscription_id, r1.batch.batch_id, r1.batch.lease_owner
        )

        # 区域收窄到 west；字段维度不传应保持上一版本
        self.service.adjust_rule(sub.subscription_id, areas={"west"}, reason="区域调整")
        loaded = self.store.get_subscription(sub.subscription_id)
        self.assertEqual(loaded.current_rule.version, 2)
        self.assertEqual(loaded.current_rule.areas, frozenset({"west"}))
        self.assertEqual(
            loaded.current_rule.fields,
            frozenset({"station_id", "status", "operator_note"}),
        )

        self.emit("e2", area="east")
        self.emit("w1", area="west")
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r2.batch.rule_version, 2)
        self.assertEqual(r2.batch.event_ids, ("w1",))
        # 存量批次保留生成时的规则版本号
        self.assertEqual(self.store.get_batch(r1.batch.batch_id).rule_version, 1)

    def test_explicit_none_widens_area(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0, areas={"east"})
        self.service.adjust_rule(sub.subscription_id, areas=None)
        self.emit("w", area="west")
        self.emit("e", area="east")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r.batch.event_ids, ("w", "e"))

    def test_rule_history_preserved(self) -> None:
        pid, _k, _s = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, areas={"east"})
        self.service.adjust_rule(sub.subscription_id, areas={"west"})
        self.service.adjust_rule(sub.subscription_id, areas={"north"})
        reloaded = self.store.get_subscription(sub.subscription_id)
        self.assertEqual([r.version for r in reloaded.rules], [1, 2, 3])
        self.assertEqual(
            [r.areas for r in reloaded.rules],
            [frozenset({"east"}), frozenset({"west"}), frozenset({"north"})],
        )
