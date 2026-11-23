"""Service loop against a private redis-server: table_1 rows in, table_2 / result out."""

import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import time

import pandas as pd
import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))

from tools.tcpinfo_csv import snapshots  # noqa: E402

MODEL_DIR = os.environ.get("TURBOTEST_MODEL_DIR", os.path.join(HERE, "..", "..", "..", "model_shared"))
REDIS_BIN = os.environ.get("REDIS_SERVER_BIN") or shutil.which("redis-server") or os.path.join(
    HERE, "..", "..", "..", "redis_local", "redis-stable", "src", "redis-server")
pytestmark = pytest.mark.skipif(
    not (os.path.isdir(MODEL_DIR) and os.path.exists(REDIS_BIN)), reason="needs model_shared and redis-server")


@pytest.fixture(scope="module")
def rds():
    import redis

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    p = subprocess.Popen([REDIS_BIN, "--port", str(port), "--save", "", "--appendonly", "no"],
                         stdout=subprocess.DEVNULL)
    r = redis.Redis(port=port)
    for _ in range(50):
        try:
            r.ping()
            break
        except Exception:
            time.sleep(0.1)
    yield r
    p.terminate()


@pytest.fixture(scope="module")
def service(rds):
    from turbotest.models import TurboTestModels
    from turbotest.service import Service

    return Service(rds, TurboTestModels(MODEL_DIR, 15, "onnx", epsilons=[5, 35]), poll_ms=1)


def _rows(path, n=1200):
    snaps = snapshots(pd.read_csv(path, low_memory=False))[:n]
    def f(x):
        return "" if x != x else str(int(x))
    return [",".join([str(s.ts_ns), f(s.bytes_acked), f(s.rtt), f(s.snd_cwnd), f(s.bytes_sent),
                      f(s.bytes_retrans), f(s.retransmits), f(s.cwnd_gain)]) for s in snaps]


def _steady_rows(mbps=100, n=1000, rtt_us=20000):
    """A steady flow: one snapshot every 10 ms at a constant rate."""
    t0 = time.time_ns()
    per = int(mbps * 1e6 / 8 * 0.01)
    out = []
    for i in range(n):
        acked = per * i
        out.append(f"{t0 + i * 10_000_000},{acked},{rtt_us},120,{acked + 250000},0,0,256")
    return out


@pytest.mark.parametrize("eps", [5, 15, 35])
def test_job_produces_consistent_outputs(rds, service, eps):
    path = sorted(glob.glob(os.path.join(HERE, "data", "*.csv.gz")))[0]
    uuid = f"test-{eps}"
    rds.delete(f"table_1:{uuid}", f"table_2:{uuid}", f"turbotest:result:{uuid}", f"turbotest:done:{uuid}",
               f"turbotest:shadow:{uuid}")
    rds.set(f"table_2:{uuid}", 0)
    rds.rpush("turbotest:queue", json.dumps({"uuid": uuid, "mode": "shadow", "epsilon": eps}))
    rows = _rows(path)
    # Deliver rows in small chunks, as tcp-info would, and step the service.
    for i in range(0, len(rows), 7):
        rds.rpush(f"table_1:{uuid}", *rows[i:i + 7])
        service.step()
    rds.set(f"turbotest:done:{uuid}", 1)
    service.step()
    res = {k.decode(): v.decode() for k, v in rds.hgetall(f"turbotest:result:{uuid}").items()}
    assert res["status"] in ("stop", "no_stop")
    assert int(res["epsilon"]) == eps
    flag = int(rds.get(f"table_2:{uuid}"))
    assert flag == (1 if res["status"] == "stop" else 0)
    if res["status"] == "stop":
        assert int(res["tau"]) % 5 == 0 and float(res["predicted_mbps"]) >= 0
    assert uuid not in service.jobs
    shadow = {int(k): json.loads(v) for k, v in rds.hgetall(f"turbotest:shadow:{uuid}").items()}
    assert sorted(shadow) == sorted(service.models.classifiers)
    assert shadow[eps]["decision"] == res["status"]
    if res["status"] == "stop":
        assert shadow[eps]["tau"] == int(res["tau"])


def test_stale_job_dropped(rds, service):
    old = time.time_ns() - 60 * 10**9
    rds.rpush("turbotest:queue", json.dumps({"uuid": "stale-1", "mode": "enforce", "enqueued_ns": old}))
    service.step()
    assert "stale-1" not in service.jobs


def test_wrongtype_key_does_not_stall(rds, service):
    rds.delete("table_1:bad-1")
    rds.set("table_1:bad-1", "not a list")
    rds.rpush("turbotest:queue", json.dumps({"uuid": "bad-1", "mode": "enforce"}))
    rds.rpush("turbotest:queue", json.dumps({"uuid": "ok-1", "mode": "enforce"}))
    service.step()
    service.step()  # must not raise
    assert "ok-1" in service.jobs
    rds.set("turbotest:done:bad-1", 1)
    rds.set("turbotest:done:ok-1", 1)
    service.step()
    assert "bad-1" not in service.jobs and "ok-1" not in service.jobs


def test_failed_write_is_retried(rds, service, monkeypatch):
    """A decision whose Redis write fails must be written again, not lost."""
    import redis as redislib

    real_pipeline = service.r.pipeline
    fail = {"on": False}

    class FailingPipe:
        def __init__(self, p):
            self.p = p

        def __getattr__(self, k):
            return getattr(self.p, k)

        def execute(self, *a, **kw):
            if fail["on"] and not kw:  # the write pipeline (reads pass raise_on_error)
                raise redislib.exceptions.ConnectionError("boom")
            return self.p.execute(*a, **kw)

    monkeypatch.setattr(service.r, "pipeline", lambda *a, **kw: FailingPipe(real_pipeline(*a, **kw)))
    for k, path in enumerate(["steady"] + sorted(glob.glob(os.path.join(HERE, "data", "*.csv.gz")))):
        uuid = f"retry-{k}"
        rds.delete(f"table_1:{uuid}", f"table_2:{uuid}", f"turbotest:result:{uuid}", f"turbotest:done:{uuid}")
        rds.rpush("turbotest:queue", json.dumps({"uuid": uuid, "mode": "enforce", "epsilon": 35}))
        fail["on"] = False
        service.step()
        fail["on"] = True
        rows = _steady_rows() if path == "steady" else _rows(path)
        for i in range(0, len(rows), 7):
            rds.rpush(f"table_1:{uuid}", *rows[i:i + 7])
            try:
                service.step()
            except redislib.exceptions.ConnectionError:
                assert uuid in service.jobs and service.jobs[uuid].unwritten
                fail["on"] = False
                service.step()
                assert int(rds.get(f"table_2:{uuid}")) == 1
                assert uuid not in service.jobs
                return
        fail["on"] = False
        rds.set(f"turbotest:done:{uuid}", 1)
        service.step()
    pytest.skip("classifier did not stop on any fixture recording")
