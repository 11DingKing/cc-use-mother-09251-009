"""并发：并发确认只生效一次、并发拉取只产生一个在途批次、并发摄入幂等。"""
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from helpers import ingest, make_service, make_subscription

WORKERS = 8


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.service, self.clock = make_service(f"{self._tmp.name}/t.db")
        self.partner, self.sub = make_subscription(self.service)
        self.pid = self.partner["partner_id"]
        self.sid = self.sub["id"]

    def tearDown(self):
        self.service.close()
        self._tmp.cleanup()

    def test_concurrent_acks_confirm_and_bill_once(self):
        """并发确认同一批次：恰好一次生效，计费不重复，游标只推进一次。"""
        ingest(self.service, 5)
        batch = self.service.pull(self.pid, self.sid)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            results = list(pool.map(
                lambda _: self.service.ack(self.pid, self.sid, batch["batch_id"]),
                range(WORKERS),
            ))

        confirmed = [r for r in results if not r["duplicate"]]
        duplicates = [r for r in results if r["duplicate"]]
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(len(duplicates), WORKERS - 1)
        self.assertTrue(all(r["cursor"] == 5 for r in results))
        self.assertEqual(len(self.service.billing_report(self.sid)), 5)
        self.assertEqual(self.service.cursor_of(self.sid), 5)

    def test_concurrent_pulls_yield_single_inflight_batch(self):
        """并发拉取：只创建一个在途批次，事件只计费一次。"""
        ingest(self.service, 4)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            batches = list(pool.map(
                lambda _: self.service.pull(self.pid, self.sid), range(WORKERS)
            ))

        self.assertEqual(len({b["batch_id"] for b in batches}), 1)
        self.assertEqual(len(self.service.list_batches(self.sid)), 1)
        self.assertEqual(sum(b["events_billed"] for b in batches), 4)
        self.assertEqual(len(self.service.billing_report(self.sid)), 4)

    def test_concurrent_ingest_same_event_deduped(self):
        """并发摄入同一 event_id：恰好一条入日志。"""
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            results = list(pool.map(
                lambda _: self.service.ingest_event("e1", "s1", "east", {"a": 1}),
                range(WORKERS),
            ))

        created = [r for r in results if not r["duplicate"]]
        self.assertEqual(len(created), 1)
        self.assertEqual(len({r["version"] for r in results}), 1)


if __name__ == "__main__":
    unittest.main()
