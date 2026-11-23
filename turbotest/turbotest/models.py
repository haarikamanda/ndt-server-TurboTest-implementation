"""TurboTest Stage 1 (XGBoost regressor) and Stage 2 (Transformer classifier).

The input construction here mirrors the offline code exactly:

* Regressor: model_shared/xgboost_regressor/code/1_million_throughput_faster.py
  (get_features, feature_window=20, taus 5..100 step 5, float32 features,
  StandardScaler, prediction in bit/s).
* Classifier: model_shared/transformer_classifier/code/1_million_analysis/
  1_pre_process_chunked.py (13 features + cumulative average throughput per
  100 ms row, then the regressor predictions in Mbit/s, padded with -1 /
  truncated to 1300 values) and 2_attention_test_classifier_regressor_outputs.py
  (reshape to 13x100, min/range normalization, reshape to 100x13, nan_to_num,
  transpose, sigmoid output, threshold 0.5).
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np

from .features import CLASSIFIER_COL_IDX, COL

REGRESSOR_TAUS = list(range(5, 101, 5))
CLASSIFIER_TAUS = list(range(5, 100, 5))  # 5..95, as in 1_pre_process_chunked.py
FEATURE_WINDOW = 20
PAD_LENGTH = 1300
PAD_VALUE = -1.0

_REG_COLS = [COL["sum_packet_length"], COL["avg_packet_length"], COL["stddev_packet_length"]]
_SUM_PKT = COL["sum_packet_length"]

EPSILON_CHECKPOINTS = {
    5: "2025_09_05_classifier_BCE_oracle_05_regressor_output_epoch5.pt",
    10: "2025_09_05_classifier_BCE_oracle_10_regressor_output_epoch5.pt",
    15: "2025_09_05_classifier_BCE_oracle_15_regressor_output_epoch5_test.pt",
    20: "2025_09_05_classifier_BCE_oracle_20_regressor_output_epoch5.pt",
    25: "2025_09_05_classifier_BCE_oracle_25_regressor_output_epoch5.pt",
    30: "2025_09_05_classifier_BCE_oracle_30_regressor_output_epoch5.pt",
    35: "2025_09_05_classifier_BCE_oracle_35_regressor_output_epoch5.pt",
}


def regressor_input(m: np.ndarray, tau: int, feature_window: int = FEATURE_WINDOW) -> np.ndarray:
    """Same vector as get_features(): column-major window, then padded rows.

    m: (n_rows, 13) feature matrix (FEATURE_CSV_COLUMNS order), n_rows >= tau.
    """
    length = min(feature_window, tau)
    start = max(0, tau - feature_window)
    temp = m[start:tau, _REG_COLS].astype(np.float32)  # CSV read with dtype float32
    parts = [temp[:, 0], temp[:, 1], temp[:, 2]]
    pad_rows = feature_window - length
    if pad_rows > 0:
        parts.append(np.tile(temp[-1, :], pad_rows))
    return np.concatenate(parts).astype(np.float32)


def classifier_input(m: np.ndarray, tau_curr: int, reg_pred_bps: Dict[int, float]) -> np.ndarray:
    """The raw 1300-value vector of 1_pre_process_chunked.process_batch()."""
    seq = m[:tau_curr, CLASSIFIER_COL_IDX]
    pkt = m[:tau_curr, _SUM_PKT]
    thr_bin = 8.0 * pkt / 0.1
    cum_avg_thr = np.cumsum(thr_bin) / np.arange(1, len(thr_bin) + 1)
    seq14 = np.concatenate([seq, (cum_avg_thr / 1e6)[:, None]], axis=1)
    vec = seq14.astype(np.float32).reshape(-1)
    vec = np.nan_to_num(vec, nan=0.0)
    preds = np.array([reg_pred_bps[t] / 1e6 for t in range(5, tau_curr + 1, 5)], dtype=np.float32)
    vec = np.concatenate([vec, preds])
    if vec.size < PAD_LENGTH:
        vec = np.concatenate([vec, np.full(PAD_LENGTH - vec.size, PAD_VALUE, dtype=vec.dtype)])
    else:
        vec = vec[:PAD_LENGTH]
    return vec


def normalize_classifier_input(vec: np.ndarray, min_vals: np.ndarray, ranges: np.ndarray) -> np.ndarray:
    """(1300,) raw vector -> (13, 100) model input, as the test script does."""
    x = (vec.reshape(13, 100) - min_vals) / ranges
    x = x.reshape(100, 13)
    x = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
    return np.ascontiguousarray(x.T, dtype=np.float32)


class Regressor:
    """Stage 1: XGBoost throughput regressor (prediction_interval = 5)."""

    def __init__(self, model_dir: str):
        import joblib

        d = os.path.join(model_dir, "xgboost_regressor", "model", "saved_paths_08_28_2025_throughput_5.3")
        self.scaler = joblib.load(os.path.join(d, "xg_all_scaler_5.pkl"))
        self.model = joblib.load(os.path.join(d, "xg_regressor_5.pkl"))
        # Single-threaded prediction: lowest latency for one row, no thread fan-out.
        try:
            self.model.set_params(n_jobs=1)
        except Exception:
            pass
        self.booster = self.model.get_booster()

    def predict_bps(self, x: np.ndarray) -> float:
        xs = self.scaler.transform(x.reshape(1, -1))
        return float(self.booster.inplace_predict(xs)[0])

    def predict_bps_batch(self, xs: np.ndarray) -> np.ndarray:
        return self.booster.inplace_predict(self.scaler.transform(xs)).astype(np.float32)


class Classifier:
    """Stage 2: Transformer stopping classifier for one epsilon."""

    def __init__(self, model_dir: str, epsilon: int = 15, backend: str = "onnx",
                 onnx_cache_dir: Optional[str] = None, threads: int = 1):
        tc = os.path.join(model_dir, "transformer_classifier")
        norm = os.path.join(tc, "saved_normalization_values")
        self.min_vals = np.load(os.path.join(norm, "min_vals.npy")).astype(np.float32)
        self.ranges = np.load(os.path.join(norm, "ranges.npy")).astype(np.float32)
        self.epsilon = epsilon
        self.checkpoint = self.checkpoint_path(model_dir, epsilon)
        self.backend = backend
        self._torch_model = None
        self._ort = None
        if backend == "onnx":
            onnx_path = export_onnx(self.checkpoint, onnx_cache_dir)
            import onnxruntime as ort

            so = ort.SessionOptions()
            so.intra_op_num_threads = threads
            so.inter_op_num_threads = 1
            so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
            self._ort = ort.InferenceSession(onnx_path, so, providers=["CPUExecutionProvider"])
            self._ort_in = self._ort.get_inputs()[0].name
        else:
            import torch

            torch.set_num_threads(threads)
            self._torch_model = load_torch_model(self.checkpoint)

    @staticmethod
    def checkpoint_path(model_dir: str, epsilon: int) -> str:
        return os.path.join(model_dir, "transformer_classifier", "model_paths", EPSILON_CHECKPOINTS[epsilon])

    def prob_batch(self, x: np.ndarray) -> np.ndarray:
        """x: (B, 13, 100) normalized input. Returns (B,) probabilities."""
        if self._ort is not None:
            return self._ort.run(None, {self._ort_in: x.astype(np.float32)})[0].reshape(-1)
        import torch

        with torch.no_grad():
            return self._torch_model(torch.from_numpy(x)).numpy().reshape(-1)

    def prob(self, vec: np.ndarray) -> float:
        x = normalize_classifier_input(vec, self.min_vals, self.ranges)[None]
        return float(self.prob_batch(x)[0])


def load_torch_model(checkpoint: str):
    """Load a pickled TSTransformerEncoder checkpoint on CPU."""
    import torch

    from . import transformer_model_classifier

    # The checkpoints pickle the class as "transformer_model_classifier.TSTransformerEncoder".
    sys.modules.setdefault("transformer_model_classifier", transformer_model_classifier)
    model = torch.load(checkpoint, map_location="cpu", weights_only=False)
    # Training wrapped the model in nn.DataParallel; unwrap it for CPU/ONNX inference.
    if isinstance(model, torch.nn.DataParallel):
        model = model.module
    model.eval()
    return model


def export_onnx(checkpoint: str, cache_dir: Optional[str] = None) -> str:
    """Export the checkpoint to ONNX once and reuse the file afterwards."""
    cache_dir = cache_dir or os.path.join(os.path.expanduser("~"), ".cache", "turbotest")
    os.makedirs(cache_dir, exist_ok=True)
    out = os.path.join(cache_dir, os.path.basename(checkpoint).replace(".pt", ".onnx"))
    if os.path.exists(out) and os.path.getmtime(out) >= os.path.getmtime(checkpoint):
        return out
    import torch

    model = load_torch_model(checkpoint)
    dummy = torch.zeros(1, 13, 100)
    tmp = out + ".tmp"
    torch.onnx.export(
        model, dummy, tmp, input_names=["x"], output_names=["prob"],
        dynamic_axes={"x": {0: "batch"}, "prob": {0: "batch"}}, opset_version=17,
        dynamo=False,
    )
    os.replace(tmp, out)
    return out


@dataclass
class Decision:
    tau: int                    # number of 100 ms rows seen when the classifier said stop
    predicted_bps: float        # Stage 1 prediction at tau (bit/s)
    probability: float          # Stage 2 output at tau
    elapsed_ms: float           # wall time since the first snapshot of the flow


class TurboTestModels:
    """Both stages, shared by all concurrent tests in one process.

    One regressor (Stage 1 does not depend on epsilon) and one classifier per
    loaded epsilon (Stage 2 is trained per epsilon). Each test picks its epsilon.
    """

    def __init__(self, model_dir: str, epsilon: int = 15, backend: str = "onnx",
                 threshold: float = 0.5, onnx_cache_dir: Optional[str] = None,
                 epsilons: Optional[list] = None):
        self.regressor = Regressor(model_dir)
        self.epsilon = epsilon  # default for tests that do not ask for one
        eps = sorted(set((epsilons or []) + [epsilon]))
        self.classifiers = {e: Classifier(model_dir, e, backend, onnx_cache_dir) for e in eps}
        self.threshold = threshold

    @property
    def classifier(self) -> "Classifier":
        return self.classifiers[self.epsilon]


class TurboTestSession:
    """Per-test state: streaming features, regressor history, decision."""

    def __init__(self, models: TurboTestModels, epsilon: Optional[int] = None):
        from .features import StreamingFeatureBuilder

        self.models = models
        self.epsilon = epsilon if epsilon in models.classifiers else models.epsilon
        self.classifier = models.classifiers[self.epsilon]
        self.features = StreamingFeatureBuilder()
        self.reg_pred_bps: Dict[int, float] = {}
        self.probs: Dict[int, float] = {}
        self.decision: Optional[Decision] = None
        self.timings_ms = []  # (tau, regressor_ms, classifier_ms)

    def add(self, snapshot) -> Optional[Decision]:
        """Feed one snapshot. Returns a Decision the first time the classifier says stop."""
        if self.features.add(snapshot):
            return self._on_new_row()
        return None

    def flush(self) -> Optional[Decision]:
        if self.features.flush():
            return self._on_new_row()
        return None

    def tick(self, now_ns: int, margin_ns: int = 50_000_000) -> Optional[Decision]:
        """Close the open bin by wall clock (see StreamingFeatureBuilder.close_until)."""
        if self.features.close_until(now_ns, margin_ns):
            return self._on_new_row()
        return None

    def _on_new_row(self) -> Optional[Decision]:
        n = self.features.n_rows
        if n % 5 != 0 or n > 100:
            return None
        m = self.features.matrix()
        t0 = time.perf_counter()
        self.reg_pred_bps[n] = self.models.regressor.predict_bps(regressor_input(m, n))
        t1 = time.perf_counter()
        c_ms = 0.0
        out = None
        if self.decision is None and n in CLASSIFIER_TAUS:
            p = self.classifier.prob(classifier_input(m, n, self.reg_pred_bps))
            c_ms = (time.perf_counter() - t1) * 1e3
            self.probs[n] = p
            if p >= self.models.threshold:
                last_ts = self.features.start_ns + self.features.bin_index[-1] * 100_000_000
                self.decision = Decision(n, self.reg_pred_bps[n], p,
                                         (last_ts - self.features.start_ns) / 1e6 + 100.0)
                out = self.decision
        self.timings_ms.append((n, (t1 - t0) * 1e3, c_ms))
        return out
