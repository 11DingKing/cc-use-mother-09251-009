"""突发拥堵优先级、运营回放与审计。"""
from __future__ import annotations

from service_09251_009.domain import models as m

from _base import ServiceTestBase


class PriorityReplayAuditTests(ServiceTestBase):
    def test_congested_batch_jumps_queue(self) -> None:
        # pA、pB 先产生普通待投递批次
        subs = {}
        secrets_map = {}
        for pid in ("pA", "pB"):
            self.service.register_partner(pid, pid)
            _kid, sec = self.service.issue_api_key(pid)
            secrets_map[pid] = sec
            subs[pid] = self.service.create_subscription(pid, refresh_interval=0)

        self.emit("a1", station_id="sa")
        self.emit("b1", station_id="sb")
        self.service.materialize_due()
        queue = self.service.delivery_queue()
        self.assertEqual(
            [b.subscription_id for b in queue],
            [subs["pA"].subscription_id, subs["pB"].subscription_id],
        )

        # 新合作方 pC 出现突发拥堵，其批次应抢占队首
        self.service.register_partner("pC", "pC")
        _kid, sec = self.service.issue_api_key("pC")
        subs["pC"] = self.service.create_subscription("pC", refresh_interval=0)
        self.emit("c1", station_id="sc", congestion=m.CongestionLevel.CRITICAL)
        created = self.service.materialize_due()
        self.assertEqual([b.subscription_id for b in created],
                         [subs["pC"].subscription_id])
        queue = self.service.delivery_queue()
        head = queue[0]
        self.assertEqual(head.subscription_id, subs["pC"].subscription_id)
        self.assertEqual(head.priority, m.CongestionLevel.CRITICAL.priority)

    def test_dispatch_next_delivers_priority_first_without_double_billing(self) -> None:
        self.service.register_partner("pA", "A")
        _k1, _s1 = self.service.issue_api_key("pA")
        sub_a = self.service.create_subscription(
            "pA", refresh_interval=0, areas={"east"}
        )
        self.service.register_partner("pC", "C")
        _k2, secret_c = self.service.issue_api_key("pC")
        sub_c = self.service.create_subscription(
            "pC", refresh_interval=0, areas={"north"}
        )
        self.emit("a", station_id="sa", area="east")
        self.emit("c", station_id="sc", area="north",
                  congestion=m.CongestionLevel.CRITICAL)
        self.service.materialize_due()

        # 推送派发：拥堵批次先出
        first = self.service.dispatch_next()
        self.assertEqual(first.subscription_id, sub_c.subscription_id)
        self.assertEqual(first.state, m.LeaseState.DELIVERED)
        second = self.service.dispatch_next()
        self.assertEqual(second.subscription_id, sub_a.subscription_id)
        self.assertIsNone(self.service.dispatch_next())
        self.assertEqual(self.store.billing_count(), 2)

        # 同批次经拉取路径再次出现只返回原租约，不重复计费
        again = self.service.fetch_batch("pC", secret_c, sub_c.subscription_id)
        self.assertEqual(again.batch.batch_id, first.batch_id)
        self.assertEqual(self.store.billing_count(), 2)

    def test_replay_resets_cursor_and_supersedes_old_batch(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(
            pid, refresh_interval=0,
            fields={"station_id", "status", "operator_note"},
        )
        for i in range(4):
            self.emit(f"e{i}")
        r1 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual(r1.batch.event_ids, ("e0", "e1", "e2", "e3"))
        # 合作方称只处理了前两条，运营回放到 seq=2
        self.service.replay(sub.subscription_id, from_seq=2, actor="ops-li")
        self.assertEqual(
            self.store.get_batch(r1.batch.batch_id).state, m.LeaseState.SUPERSEDED
        )
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertNotEqual(r2.batch.batch_id, r1.batch.batch_id)
        self.assertEqual(r2.batch.from_seq, 2)
        self.assertEqual(r2.batch.event_ids, ("e2", "e3"))

        # 回放重投不重复计费
        self.assertEqual(self.store.billing_count(), 4)

    def test_replay_from_zero_redelivers_everything_once_billed(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        for i in range(3):
            self.emit(f"e{i}")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.service.ack_batch(
            pid, secret, sub.subscription_id, r.batch.batch_id, r.batch.lease_owner
        )
        self.assertEqual(self.store.billing_count(), 3)
        self.service.replay(sub.subscription_id, from_seq=0)
        r2 = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.assertEqual([ev["event_id"] for ev in r2.batch.to_wire()["events"]],
                         ["e0", "e1", "e2"])
        self.assertEqual(self.store.billing_count(), 3)  # 不重复计费

    def test_audit_trail_records_lifecycle(self) -> None:
        pid, kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0, areas={"east"})
        self.emit("e1")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        self.service.ack_batch(
            pid, secret, sub.subscription_id, r.batch.batch_id, r.batch.lease_owner
        )
        self.service.adjust_rule(sub.subscription_id, areas={"west"}, reason="扩区")
        self.service.replay(sub.subscription_id, from_seq=1)

        actions = [e["action"] for e in self.service.audit_trail(
            subscription_id=sub.subscription_id)]
        self.assertIn("subscription_created", actions)
        self.assertIn("batch_fetched", actions)
        self.assertIn("batch_acked", actions)
        self.assertIn("rule_adjusted", actions)
        self.assertIn("cursor_reset", actions)
        self.assertIn("batch_replayed", actions)

        # 可按合作方与动作过滤
        partner_events = self.service.audit_trail(partner_id=pid)
        self.assertTrue(all(e["partner_id"] == pid for e in partner_events))
        fetched = self.service.audit_trail(action=m.AuditAction.BATCH_FETCHED)
        self.assertTrue(fetched and all(e["action"] == "batch_fetched" for e in fetched))

        # 轮换/撤销记录在合作方维度
        self.service.rotate_api_key(pid)
        self.service.revoke_partner(pid, reason="审计抽查")
        partner_actions = [e["action"] for e in
                           self.service.audit_trail(partner_id=pid)]
        self.assertIn("key_issued", partner_actions)
        self.assertIn("key_revoked", partner_actions)
        self.assertIn("partner_revoked", partner_actions)

    def test_billing_detail_is_keyed_by_fingerprint(self) -> None:
        pid, _kid, secret = self.given_partner_with_key()
        sub = self.service.create_subscription(pid, refresh_interval=0)
        self.emit("e1")
        r = self.service.fetch_batch(pid, secret, sub.subscription_id)
        fp = m.billing_fingerprint(sub.subscription_id, "e1")
        entries = self.store.list_billing(sub.subscription_id)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["fingerprint"], fp)
        self.assertEqual(entries[0]["batch_id"], r.batch.batch_id)
