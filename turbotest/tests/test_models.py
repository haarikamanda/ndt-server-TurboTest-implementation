"""Engine-level checks that need the model files (skipped without them)."""

import os
import sys

import numpy as np
import pandas as pd
import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))

from turbotest.features import features_from_dataframe  # noqa: E402
from turbotest.models import (  # noqa: E402
    CLASSIFIER_TAUS, TurboTestModels, TurboTestSession, classifier_input, regressor_input,
)

MODEL_DIR = os.environ.get("TURBOTEST_MODEL_DIR", os.path.join(HERE, "..", "..", "..", "model_shared"))
FEAT_DIR = os.path.join(MODEL_DIR, "input_data", "tcpinfo_features_09_12_2025")
pytestmark = pytest.mark.skipif(not os.path.isdir(FEAT_DIR), reason="model_shared not available")


@pytest.fixture(scope="module")
def models():
    return TurboTestModels(MODEL_DIR, 15, "onnx")


def _batch_decision(models, m):
    """The offline_eval decision rule, written out independently."""
    n = m.shape[0]
    preds = {t: models.regressor.predict_bps(regressor_input(m, t)) for t in range(5, 101, 5) if n >= t}
    for t in CLASSIFIER_TAUS:
        if n >= t and models.classifier.prob(classifier_input(m, t, preds)) >= models.threshold:
            return t, preds[t]
    return None, None


def test_session_matches_batch(models):
    files = sorted(os.listdir(FEAT_DIR))[::100003][:10]
    for fn in files:
        m = features_from_dataframe(pd.read_csv(os.path.join(FEAT_DIR, fn)))
        s = TurboTestSession(models)
        d = None
        for i in range(m.shape[0]):  # feed rows as the streaming builder would close them
            s.features.rows.append(m[i])
            s.features.bin_index.append(i)
            s.features.start_ns = 0
            d = d or s._on_new_row()
        want_tau, want_pred = _batch_decision(models, m)
        assert (d.tau if d else None) == want_tau, fn
        if d:
            assert d.predicted_bps == pytest.approx(want_pred, rel=1e-6)


def test_latency_budget(models):
    """Paper §5.6: both stages must be far below the 500 ms decision stride."""
    import time

    m = np.abs(np.random.default_rng(0).normal(1e5, 3e4, size=(100, 13)))
    preds = {t: models.regressor.predict_bps(regressor_input(m, t)) for t in range(5, 101, 5)}
    ts = []
    for _ in range(200):
        t0 = time.perf_counter()
        models.regressor.predict_bps(regressor_input(m, 50))
        models.classifier.prob(classifier_input(m, 50, preds))
        ts.append(time.perf_counter() - t0)
    assert np.percentile(ts, 99) < 0.050


def test_batched_equals_immediate(models):
    """The service's cross-test batching must not change any decision."""
    from turbotest.models import run_batched

    files = sorted(os.listdir(FEAT_DIR))[7::50021][:12]
    mats = [features_from_dataframe(pd.read_csv(os.path.join(FEAT_DIR, fn))) for fn in files]
    imm = [TurboTestSession(models) for _ in files]
    dfr = [TurboTestSession(models, deferred=True) for _ in files]
    got = {}
    for i in range(max(m.shape[0] for m in mats)):
        for k, m in enumerate(mats):
            if i < m.shape[0]:
                imm[k].add_bin_row(m[i])
                dfr[k].add_bin_row(m[i])
        for s, d in run_batched(models, dfr):
            got[id(s)] = d
    for a, b in zip(imm, dfr):
        assert (a.decision.tau if a.decision else None) == (b.decision.tau if b.decision else None)
        for t, p in a.probs.items():
            assert b.probs[t] == pytest.approx(p, abs=1e-5)
