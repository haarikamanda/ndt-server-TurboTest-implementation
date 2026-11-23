"""TurboTest real-time inference service.

Redis protocol (shared with ndt-server and tcp-info):

  turbotest:queue            LIST   ndt-server RPUSHes a JSON job when a download
                                    with turbotest=enforce|shadow starts:
                                    {"uuid": "...", "mode": "enforce", "start_ns": 1700000000000000000}
  table_1:<uuid>             LIST   tcp-info RPUSHes one row per archived snapshot:
                                    ts_ns,BytesAcked,RTT,SndCwnd,BytesSent,BytesRetrans,Retransmits,CwndGain
  table_2:<uuid>             STRING termination flag. ndt-server sets "0" at start,
                                    this service sets "1" when the classifier says stop.
  turbotest:result:<uuid>    HASH   decision details (tau, predicted_mbps, probability,
                                    decided_at_ns, ...) written together with table_2.
  turbotest:done:<uuid>      STRING ndt-server sets it when the download ends; the
                                    service then drops the session.

One loop serves all tests: every --poll-ms it fetches the new table_1 rows of
every active test with a single pipelined round trip, advances each test's
streaming features, runs Stage 1 / Stage 2 when a 5th 100 ms row closes, and
writes decisions back in the same way.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Dict, List

import redis

from .features import Snapshot
from .models import TurboTestModels, TurboTestSession

log = logging.getLogger("turbotest")

QUEUE_KEY = "turbotest:queue"
T1 = "table_1:"
T2 = "table_2:"
RESULT = "turbotest:result:"
DONE = "turbotest:done:"
STATS_KEY = "turbotest:stats"


@dataclass
class Job:
    uuid: str
    mode: str
    registered_ns: int
    session: TurboTestSession
    offset: int = 0                 # next table_1 index to read
    finished: bool = False
    infer_ms: List[float] = field(default_factory=list)


class Service:
    def __init__(self, r: redis.Redis, models: TurboTestModels, poll_ms: float = 5.0,
                 max_test_s: float = 16.0, key_ttl_s: int = 300, clock_close_ms: float = 50.0):
        self.r = r
        self.models = models
        self.poll_s = poll_ms / 1e3
        self.max_test_ns = int(max_test_s * 1e9)
        self.ttl = key_ttl_s
        self.clock_margin_ns = int(clock_close_ms * 1e6)
        self.jobs: Dict[str, Job] = {}
        self.running = True
        self.n_decisions = 0
        self.n_jobs = 0

    # ---- job intake -------------------------------------------------------
    def _accept_jobs(self) -> None:
        raw = self.r.lpop(QUEUE_KEY, 64)
        for item in raw or []:
            try:
                j = json.loads(item)
                uuid = j["uuid"]
            except Exception:
                log.warning("bad job %r", item)
                continue
            if uuid in self.jobs:
                continue
            self.jobs[uuid] = Job(uuid, j.get("mode", "enforce"), time.time_ns(), TurboTestSession(self.models))
            self.n_jobs += 1
            log.info("job %s mode=%s", uuid, j.get("mode"))

    # ---- one poll cycle ---------------------------------------------------
    def step(self) -> None:
        self._accept_jobs()
        if not self.jobs:
            return
        jobs = list(self.jobs.values())
        pipe = self.r.pipeline(transaction=False)
        for jb in jobs:
            pipe.lrange(T1 + jb.uuid, jb.offset, -1)
            pipe.exists(DONE + jb.uuid)
        res = pipe.execute()
        now_ns = time.time_ns()
        writes = self.r.pipeline(transaction=False)
        any_write = False
        for i, jb in enumerate(jobs):
            rows, done = res[2 * i], res[2 * i + 1]
            jb.offset += len(rows)
            t0 = time.perf_counter()
            decision = None
            for row in rows:
                try:
                    s = Snapshot.parse(row.decode() if isinstance(row, bytes) else row)
                except ValueError:
                    continue
                d = jb.session.add(s)
                decision = decision or d
            if decision is None and jb.session.features.n_snapshots:
                decision = jb.session.tick(now_ns, self.clock_margin_ns)
            if decision is not None:
                jb.infer_ms.append((time.perf_counter() - t0) * 1e3)
                self._write_decision(writes, jb, decision)
                any_write = True
            age = now_ns - jb.registered_ns
            if done or jb.session.decision is not None or age > self.max_test_ns:
                if jb.session.decision is None and (done or age > self.max_test_ns):
                    writes.hset(RESULT + jb.uuid, mapping=self._no_stop_fields(jb))
                    writes.expire(RESULT + jb.uuid, self.ttl)
                    any_write = True
                jb.finished = True
        if any_write:
            writes.execute()
        for jb in jobs:
            if jb.finished:
                self.jobs.pop(jb.uuid, None)

    def _write_decision(self, pipe, jb: Job, d) -> None:
        now = time.time_ns()
        s = jb.session
        timings = s.timings_ms
        fields = {
            "uuid": jb.uuid,
            "mode": jb.mode,
            "status": "stop",
            "epsilon": self.models.epsilon,
            "tau": d.tau,
            "stop_after_ms": f"{d.elapsed_ms:.1f}",
            "predicted_mbps": f"{d.predicted_bps / 1e6:.6f}",
            "probability": f"{d.probability:.6f}",
            "decided_at_ns": now,
            "first_snapshot_ns": s.features.start_ns or 0,
            "n_snapshots": s.features.n_snapshots,
            "regressor_ms_last": f"{timings[-1][1]:.3f}" if timings else "",
            "classifier_ms_last": f"{timings[-1][2]:.3f}" if timings else "",
            "probs": json.dumps({str(k): round(v, 4) for k, v in s.probs.items()}),
            "reg_mbps": json.dumps({str(k): round(v / 1e6, 3) for k, v in s.reg_pred_bps.items()}),
        }
        pipe.hset(RESULT + jb.uuid, mapping=fields)
        pipe.expire(RESULT + jb.uuid, self.ttl)
        pipe.set(T2 + jb.uuid, 1, ex=self.ttl)
        pipe.hincrby(STATS_KEY, "decisions", 1)
        self.n_decisions += 1
        log.info("STOP %s tau=%d (%.0f ms) pred=%.2f Mbit/s p=%.3f", jb.uuid, d.tau, d.elapsed_ms,
                 d.predicted_bps / 1e6, d.probability)

    def _no_stop_fields(self, jb: Job) -> dict:
        s = jb.session
        return {
            "uuid": jb.uuid, "mode": jb.mode, "status": "no_stop", "epsilon": self.models.epsilon,
            "n_rows": s.features.n_rows, "n_snapshots": s.features.n_snapshots,
            "probs": json.dumps({str(k): round(v, 4) for k, v in s.probs.items()}),
            "reg_mbps": json.dumps({str(k): round(v / 1e6, 3) for k, v in s.reg_pred_bps.items()}),
        }

    def run(self) -> None:
        log.info("TurboTest service ready (epsilon=%d, poll=%.1f ms)", self.models.epsilon, self.poll_s * 1e3)
        while self.running:
            t0 = time.perf_counter()
            try:
                self.step()
            except redis.exceptions.ConnectionError as e:
                log.warning("redis: %s", e)
                time.sleep(1)
            dt = time.perf_counter() - t0
            if dt < self.poll_s:
                time.sleep(self.poll_s - dt)


def main() -> None:
    ap = argparse.ArgumentParser(description="TurboTest real-time early-termination service")
    ap.add_argument("--redis-addr", default=os.environ.get("REDIS_ADDR", "localhost:6379"))
    ap.add_argument("--model-dir", default=os.environ.get("TURBOTEST_MODEL_DIR", "/models"))
    ap.add_argument("--epsilon", type=int, default=int(os.environ.get("TURBOTEST_EPSILON", "15")))
    ap.add_argument("--threshold", type=float, default=float(os.environ.get("TURBOTEST_THRESHOLD", "0.5")))
    ap.add_argument("--backend", choices=["onnx", "torch"], default=os.environ.get("TURBOTEST_BACKEND", "onnx"))
    ap.add_argument("--poll-ms", type=float, default=float(os.environ.get("TURBOTEST_POLL_MS", "5")))
    ap.add_argument("--onnx-cache", default=os.environ.get("TURBOTEST_ONNX_CACHE"))
    ap.add_argument("--log-level", default=os.environ.get("TURBOTEST_LOG_LEVEL", "INFO"))
    a = ap.parse_args()
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(message)s")

    host, _, port = a.redis_addr.rpartition(":")
    r = redis.Redis(host=host or "localhost", port=int(port), socket_timeout=2)
    r.ping()
    t0 = time.time()
    models = TurboTestModels(a.model_dir, a.epsilon, a.backend, a.threshold, a.onnx_cache)
    # Warm up both stages so the first test does not pay one-time costs.
    from .models import classifier_input, regressor_input
    import numpy as np

    m = np.ones((10, 13))
    p = models.regressor.predict_bps(regressor_input(m, 5))
    models.classifier.prob(classifier_input(m, 5, {5: p}))
    log.info("models loaded in %.1fs", time.time() - t0)

    svc = Service(r, models, poll_ms=a.poll_ms)

    def stop(*_):
        svc.running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    svc.run()


if __name__ == "__main__":
    main()
