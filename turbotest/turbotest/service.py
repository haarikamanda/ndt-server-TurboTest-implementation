"""TurboTest real-time inference service.

Redis protocol (shared with ndt-server and tcp-info):

  turbotest:queue            LIST   ndt-server RPUSHes a JSON job when a download
                                    with turbotest=true starts:
                                    {"uuid": "...", "epsilon": 15, "enqueued_ns": ...}
                                    (epsilon: which Stage 2 classifier; default --epsilon)
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
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .features import Snapshot
from .models import TurboTestModels, TurboTestSession, run_batched

log = logging.getLogger("turbotest")

QUEUE_KEY = "turbotest:queue"
T1 = "table_1:"
T2 = "table_2:"
RESULT = "turbotest:result:"
DONE = "turbotest:done:"

# Prometheus metrics (exported on --metrics-port, default 9994).
M_JOBS = Counter("turbotest_jobs_total", "Tests registered by ndt-server")
M_DECISIONS = Counter("turbotest_decisions_total", "Tests finished, by outcome", ["outcome", "epsilon"])
M_ACTIVE = Gauge("turbotest_active_jobs", "Tests currently followed")
M_REJECTED = Counter("turbotest_jobs_rejected_total", "Jobs dropped (bad JSON or --max-jobs reached)", ["reason"])
M_ERRORS = Counter("turbotest_errors_total", "Errors in the service loop", ["type"])
M_STEP = Histogram("turbotest_step_seconds", "Duration of one poll cycle",
                   buckets=(.0005, .001, .002, .005, .01, .02, .05, .1, .25, .5, 1))
M_INFER = Histogram("turbotest_inference_batch_seconds", "Stage 1 + Stage 2 batch time per poll cycle",
                    buckets=(.0005, .001, .002, .005, .01, .02, .05, .1, .25, .5))
M_SNAPS = Counter("turbotest_snapshots_total", "tcp-info snapshots read from table_1")
M_BAD_ROWS = Counter("turbotest_bad_rows_total", "table_1 rows that could not be parsed")
M_LATE = Counter("turbotest_late_samples_total",
                 "Snapshots that arrived after their 100 ms bin was closed by the clock "
                 "(online features then differ from offline; raise --clock-close-ms)")


@dataclass(eq=False)  # identity: jobs are compared/collected by object
class Job:
    uuid: str
    registered_ns: int
    session: TurboTestSession
    offset: int = 0                 # next table_1 index to read
    finished: bool = False
    infer_ms: List[float] = field(default_factory=list)
    # decisions whose Redis write failed; written again next cycle
    unwritten: bool = False
    late_seen: int = 0

    def sessions(self) -> List[TurboTestSession]:
        return [self.session]


class Service:
    def __init__(self, r: redis.Redis, models: TurboTestModels, poll_ms: float = 5.0,
                 max_test_s: float = 16.0, key_ttl_s: int = 300, clock_close_ms: float = 50.0,
                 max_jobs: int = 5000, max_job_age_s: float = 5.0):
        self.r = r
        self.models = models
        self.poll_s = poll_ms / 1e3
        self.max_test_ns = int(max_test_s * 1e9)
        self.ttl = key_ttl_s
        self.clock_margin_ns = int(clock_close_ms * 1e6)
        self.max_jobs = max_jobs
        self.max_job_age_ns = int(max_job_age_s * 1e9)
        self.jobs: Dict[str, Job] = {}
        self.running = True
        self.n_decisions = 0
        self.n_jobs = 0

    # ---- job intake -------------------------------------------------------
    def _accept_jobs(self) -> None:
        raw = self.r.lpop(QUEUE_KEY, 64)
        now = time.time_ns()
        for item in raw or []:
            try:
                j = json.loads(item)
                uuid = str(j["uuid"])
                enq = int(j.get("enqueued_ns") or now)
            except Exception:
                log.warning("bad job %r", item)
                M_REJECTED.labels("bad_job").inc()
                continue
            if uuid in self.jobs:
                continue
            if now - enq > self.max_job_age_ns:
                # Queued while the service was down or behind: its download has
                # already gone on without TurboTest (or is nearly over).
                M_REJECTED.labels("stale").inc()
                continue
            if len(self.jobs) >= self.max_jobs:
                # Bounded memory/CPU: the test then simply runs to completion.
                log.warning("max jobs (%d) reached; not following %s", self.max_jobs, uuid)
                M_REJECTED.labels("max_jobs").inc()
                continue
            try:
                eps = int(j.get("epsilon") or self.models.epsilon)
            except (TypeError, ValueError):
                eps = self.models.epsilon
            sess = TurboTestSession(self.models, eps, deferred=True)
            self.jobs[uuid] = Job(uuid, enq, sess)
            self.n_jobs += 1
            M_JOBS.inc()
            log.info("job %s epsilon=%d", uuid, sess.epsilon)

    def _drop(self, jb: Job, why: str) -> None:
        """Stop following one test (its download then runs to full length)."""
        M_ERRORS.labels(why).inc()
        log.exception("dropping job %s (%s)", jb.uuid, why)
        jb.finished = True
        self.jobs.pop(jb.uuid, None)

    # ---- one poll cycle ---------------------------------------------------
    def step(self) -> None:
        self._accept_jobs()
        if not self.jobs:
            M_ACTIVE.set(0)
            return
        jobs = list(self.jobs.values())
        pipe = self.r.pipeline(transaction=False)
        for jb in jobs:
            pipe.lrange(T1 + jb.uuid, jb.offset, -1)
            pipe.exists(DONE + jb.uuid)
        # Per-key errors (e.g. WRONGTYPE) come back as values, not exceptions,
        # so one bad key cannot stall every test.
        res = pipe.execute(raise_on_error=False)
        now_ns = time.time_ns()
        writes = self.r.pipeline(transaction=False)
        written: List[Job] = []
        # 0) Decisions whose write failed last cycle.
        for jb in jobs:
            if jb.unwritten:
                self._write_all_decisions(writes, jb)
                written.append(jb)
        # 1) Ingest new snapshots; sessions only mark which taus are due.
        done_flags = {}
        for i, jb in enumerate(jobs):
            rows, done = res[2 * i], res[2 * i + 1]
            if isinstance(rows, Exception) or isinstance(done, Exception):
                M_ERRORS.labels("redis_key").inc()
                rows = [] if isinstance(rows, Exception) else rows
                done = 0 if isinstance(done, Exception) else done
            done_flags[jb.uuid] = done
            try:
                self._ingest(jb, rows, now_ns)
            except Exception:
                self._drop(jb, "ingest")
        jobs = [jb for jb in jobs if not jb.finished]
        # 2) Stage 1 + Stage 2 for every due test, batched across tests.
        t0 = time.perf_counter()
        decided = self._infer(jobs)
        batch_ms = (time.perf_counter() - t0) * 1e3
        if decided is not None:
            M_INFER.observe(batch_ms / 1e3)
        for jb, _sess, d in decided or []:
            jb.infer_ms.append(batch_ms)
            self._write_decision(writes, jb, d, batch_ms)
            if jb not in written:
                written.append(jb)
        # 3) Retire finished tests.
        for jb in jobs:
            done = done_flags[jb.uuid]
            expired = done or (now_ns - jb.registered_ns) > self.max_test_ns
            finished = expired or jb.session.decision is not None
            if not finished:
                continue
            if jb.session.decision is None:
                writes.hset(RESULT + jb.uuid, mapping=self._no_stop_fields(jb))
                writes.expire(RESULT + jb.uuid, self.ttl)
                M_DECISIONS.labels("no_stop", str(jb.session.epsilon)).inc()
            if jb not in written:
                written.append(jb)
            jb.finished = True
        if written:
            try:
                writes.execute()
                for jb in written:
                    jb.unwritten = False
            except redis.exceptions.RedisError:
                # Keep the jobs and write their decisions again next cycle;
                # a lost table_2=1 would let the test run to full length.
                for jb in written:
                    jb.unwritten = True
                    jb.finished = False
                raise
        for jb in jobs:
            if jb.finished:
                self.jobs.pop(jb.uuid, None)
        M_ACTIVE.set(len(self.jobs))

    def _ingest(self, jb: Job, rows, now_ns: int) -> None:
        jb.offset += len(rows)
        sessions = jb.sessions()
        M_SNAPS.inc(len(rows))
        for row in rows:
            try:
                snap = Snapshot.parse(row.decode() if isinstance(row, bytes) else row)
            except (ValueError, UnicodeDecodeError):
                M_BAD_ROWS.inc()
                continue
            for sess in sessions:
                sess.add(snap)
        for sess in sessions:
            if sess.features.n_snapshots:
                sess.tick(now_ns, self.clock_margin_ns)
        late = jb.session.features.late_samples
        if late > jb.late_seen:
            M_LATE.inc(late - jb.late_seen)
            jb.late_seen = late

    def _infer(self, jobs: List[Job]):
        """Batched Stage 1/2; on an error, retry per job and drop only the bad ones."""
        owner = {}
        sessions = []
        for jb in jobs:
            for sess in jb.sessions():
                owner[id(sess)] = jb
                sessions.append(sess)
        if not any(s.pending for s in sessions):
            return None
        try:
            return [(owner[id(s)], s, d) for s, d in run_batched(self.models, sessions)]
        except Exception:
            log.exception("batched inference failed; retrying per test")
        out = []
        for jb in jobs:
            try:
                out += [(jb, s, d) for s, d in run_batched(self.models, jb.sessions())]
            except Exception:
                self._drop(jb, "inference")
        return out

    def _write_all_decisions(self, pipe, jb: Job) -> None:
        if jb.session.decision is not None:
            self._write_decision(pipe, jb, jb.session.decision)

    def _write_decision(self, pipe, jb: Job, d, batch_ms: float = 0.0) -> None:
        now = time.time_ns()
        s = jb.session
        timings = s.timings_ms
        fields = {
            "uuid": jb.uuid,
            "status": "stop",
            "epsilon": s.epsilon,
            "tau": d.tau,
            "stop_after_ms": f"{d.elapsed_ms:.1f}",
            "predicted_mbps": f"{d.predicted_bps / 1e6:.6f}",
            "probability": f"{d.probability:.6f}",
            "decided_at_ns": now,
            "first_snapshot_ns": s.features.start_ns or 0,
            "n_snapshots": s.features.n_snapshots,
            "inference_batch_ms": f"{batch_ms:.3f}",
            "regressor_ms_last": f"{timings[-1][1]:.3f}" if timings else "",
            "classifier_ms_last": f"{timings[-1][2]:.3f}" if timings else "",
            "probs": json.dumps({str(k): round(v, 4) for k, v in s.probs.items()}),
            "reg_mbps": json.dumps({str(k): round(v / 1e6, 3) for k, v in s.reg_pred_bps.items()}),
        }
        pipe.hset(RESULT + jb.uuid, mapping=fields)
        pipe.expire(RESULT + jb.uuid, self.ttl)
        pipe.set(T2 + jb.uuid, 1, ex=self.ttl)
        if not jb.unwritten:
            self.n_decisions += 1
            M_DECISIONS.labels("stop", str(s.epsilon)).inc()
        log.info("STOP %s tau=%d (%.0f ms) pred=%.2f Mbit/s p=%.3f", jb.uuid, d.tau, d.elapsed_ms,
                 d.predicted_bps / 1e6, d.probability)

    def _no_stop_fields(self, jb: Job) -> dict:
        s = jb.session
        return {
            "uuid": jb.uuid, "status": "no_stop", "epsilon": s.epsilon,
            "n_rows": s.features.n_rows, "n_snapshots": s.features.n_snapshots,
            "probs": json.dumps({str(k): round(v, 4) for k, v in s.probs.items()}),
            "reg_mbps": json.dumps({str(k): round(v / 1e6, 3) for k, v in s.reg_pred_bps.items()}),
        }

    def run(self) -> None:
        log.info("TurboTest service ready (default epsilon=%d, poll=%.1f ms)", self.models.epsilon, self.poll_s * 1e3)
        while self.running:
            t0 = time.perf_counter()
            try:
                self.step()
            except redis.exceptions.RedisError as e:
                # Redis restarts or network errors: keep the jobs, retry soon.
                M_ERRORS.labels("redis").inc()
                log.warning("redis: %s", e)
                time.sleep(0.5)
            except Exception:
                # Per-test errors are handled inside step(); this is a last
                # resort so the loop itself never dies.
                M_ERRORS.labels("internal").inc()
                log.exception("service step failed")
                time.sleep(0.1)
            dt = time.perf_counter() - t0
            M_STEP.observe(dt)
            if dt < self.poll_s:
                time.sleep(self.poll_s - dt)


def main() -> None:
    ap = argparse.ArgumentParser(description="TurboTest real-time early-termination service")
    ap.add_argument("--redis-addr", default=os.environ.get("REDIS_ADDR", "localhost:6379"))
    ap.add_argument("--model-dir", default=os.environ.get("TURBOTEST_MODEL_DIR", "/models"))
    ap.add_argument("--epsilon", type=int, default=int(os.environ.get("TURBOTEST_EPSILON", "15")),
                    help="default epsilon for tests that do not ask for one")
    ap.add_argument("--epsilons", default=os.environ.get("TURBOTEST_EPSILONS", "5,10,15,20,25,30,35"),
                    help="classifiers to load; a test may request any of them")
    ap.add_argument("--threshold", type=float, default=float(os.environ.get("TURBOTEST_THRESHOLD", "0.5")))
    ap.add_argument("--backend", choices=["onnx", "torch"], default=os.environ.get("TURBOTEST_BACKEND", "onnx"))
    ap.add_argument("--poll-ms", type=float, default=float(os.environ.get("TURBOTEST_POLL_MS", "5")))
    ap.add_argument("--onnx-cache", default=os.environ.get("TURBOTEST_ONNX_CACHE"))
    ap.add_argument("--log-level", default=os.environ.get("TURBOTEST_LOG_LEVEL", "INFO"))
    ap.add_argument("--metrics-port", type=int, default=int(os.environ.get("TURBOTEST_METRICS_PORT", "9994")),
                    help="Prometheus /metrics port (0 disables)")
    ap.add_argument("--max-jobs", type=int, default=int(os.environ.get("TURBOTEST_MAX_JOBS", "5000")),
                    help="max concurrent tests; further tests run to full length")
    ap.add_argument("--max-job-age-s", type=float, default=float(os.environ.get("TURBOTEST_MAX_JOB_AGE_S", "5")),
                    help="drop queued jobs older than this (queued while the service was down)")
    ap.add_argument("--clock-close-ms", type=float, default=float(os.environ.get("TURBOTEST_CLOCK_CLOSE_MS", "50")),
                    help="close a 100 ms bin this long after its window if no later snapshot came")
    a = ap.parse_args()
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(message)s")

    host, _, port = a.redis_addr.rpartition(":")
    r = redis.Redis(host=host or "localhost", port=int(port), socket_timeout=2,
                    socket_connect_timeout=2, health_check_interval=10)
    for attempt in range(60):  # Redis may start after this container.
        try:
            r.ping()
            break
        except redis.exceptions.RedisError as e:
            log.warning("waiting for redis at %s: %s", a.redis_addr, e)
            time.sleep(1)
    else:
        raise SystemExit(f"redis at {a.redis_addr} not reachable")
    if a.metrics_port:
        start_http_server(a.metrics_port)
    t0 = time.time()
    eps = [int(e) for e in a.epsilons.split(",") if e]
    models = TurboTestModels(a.model_dir, a.epsilon, a.backend, a.threshold, a.onnx_cache, eps)
    # Warm up both stages so the first test does not pay one-time costs.
    from .models import classifier_input, regressor_input
    import numpy as np

    m = np.ones((10, 13))
    p = models.regressor.predict_bps(regressor_input(m, 5))
    for c in models.classifiers.values():
        c.prob(classifier_input(m, 5, {5: p}))
    log.info("models loaded in %.1fs (epsilons %s)", time.time() - t0, sorted(models.classifiers))

    svc = Service(r, models, poll_ms=a.poll_ms, max_jobs=a.max_jobs, max_job_age_s=a.max_job_age_s,
                  clock_close_ms=a.clock_close_ms)

    def stop(*_):
        svc.running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    svc.run()


if __name__ == "__main__":
    main()
