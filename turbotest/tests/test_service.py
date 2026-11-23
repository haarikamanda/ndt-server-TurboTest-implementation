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


@pytest.mark.parametrize("eps", [5, 15, 35])
def test_job_produces_consistent_outputs(rds, service, eps):
    path = sorted(glob.glob(os.path.join(HERE, "data", "*.csv.gz")))[0]
    uuid = f"test-{eps}"
    rds.delete(f"table_1:{uuid}", f"table_2:{uuid}", f"turbotest:result:{uuid}", f"turbotest:done:{uuid}")
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
