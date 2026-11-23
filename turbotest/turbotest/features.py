"""Streaming 100 ms feature extraction for TurboTest.

This module reproduces, sample for sample, the offline preprocessing that
produced the training and evaluation data:

    raw tcpinfo .jsonl.zst -> csvtool -> 2_features_extraction_final.py

The offline script does this per test (pandas):

    * drops the first CSV row (the metadata placeholder that csvtool emits),
    * bins snapshots into 100 ms windows relative to the first snapshot,
    * derives bytes_in_flight, ack_rtt, tcp_window_size, retransmission,
      packet_length (= positive delta of TCP.BytesAcked) and is_pipefull
      (BBR.CwndGain == 512),
    * aggregates per bin (mean / sample std / sum / max-min / any),
    * rounds every aggregate to an integer (Int64), so std of a single-sample
      bin becomes <NA>,
    * keeps only non-empty bins: a "row" is a non-empty bin, and tau counts rows.

Here the same computation happens incrementally: a bin is emitted as soon as a
snapshot from a later bin arrives (snapshots of a flow arrive in time order),
or when the caller flushes it at the end of the flow.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

import numpy as np

BIN_NS = 100_000_000  # 100 ms

# Column order of the *_features.csv files (minus time_bin).
FEATURE_CSV_COLUMNS = [
    "avg_bytes_in_flight",
    "stddev_bytes_in_flight",
    "sum_bytes_in_flight",
    "avg_ack_rtt",
    "stddev_ack_rtt",
    "sum_ack_rtt",
    "sum_packet_length",
    "stddev_packet_length",
    "avg_packet_length",
    "avg_tcp_window_size",
    "sum_retransmission",
    "sum_duplicate_ack",
    "bbrpipefull",
]
COL = {name: i for i, name in enumerate(FEATURE_CSV_COLUMNS)}

# Order that the classifier preprocessing (1_pre_process_chunked.py) uses.
CLASSIFIER_FEATURE_COLUMNS = [
    "sum_packet_length", "avg_packet_length", "stddev_packet_length",
    "sum_bytes_in_flight", "avg_bytes_in_flight", "stddev_bytes_in_flight",
    "sum_ack_rtt", "avg_ack_rtt", "stddev_ack_rtt",
    "avg_tcp_window_size", "sum_retransmission", "sum_duplicate_ack", "bbrpipefull",
]
CLASSIFIER_COL_IDX = [COL[c] for c in CLASSIFIER_FEATURE_COLUMNS]

NAN = float("nan")


@dataclass
class Snapshot:
    """The subset of one tcp-info ArchivalRecord that the features need.

    Missing values (a record without TCPInfo or without BBRInfo) are NaN,
    exactly like an empty CSV cell in the csvtool output.
    """

    ts_ns: int
    bytes_acked: float
    rtt: float
    snd_cwnd: float
    bytes_sent: float
    bytes_retrans: float
    retransmits: float
    cwnd_gain: float = NAN

    @staticmethod
    def parse(line: str) -> "Snapshot":
        """Parse the compact row that tcp-info writes to Redis.

        Format: ts_ns,BytesAcked,RTT,SndCwnd,BytesSent,BytesRetrans,Retransmits,CwndGain
        Empty fields mean "missing".
        """
        p = line.split(",")
        if len(p) < 8:
            raise ValueError(f"bad snapshot row: {line!r}")

        def f(s: str) -> float:
            return float(s) if s else NAN

        return Snapshot(int(p[0]), f(p[1]), f(p[2]), f(p[3]), f(p[4]), f(p[5]), f(p[6]), f(p[7]))


def _round_half_even(x: float) -> float:
    # pandas DataFrame.round(0) uses numpy rounding (half to even).
    if math.isnan(x):
        return NAN
    return float(np.round(x))


def _nanmean(v: Sequence[float]) -> float:
    vals = [x for x in v if not math.isnan(x)]
    return sum(vals) / len(vals) if vals else NAN


def _nanstd(v: Sequence[float]) -> float:
    # pandas groupby().std(): sample std (ddof=1) over non-NaN values, NaN if
    # fewer than 2. pandas computes the variance with Welford's online update
    # (pandas/_libs/groupby.pyx: group_var); use the same update so values
    # close to x.5 round the same way as in the training data.
    nobs = 0
    mean = 0.0
    m2 = 0.0
    for x in v:
        if math.isnan(x):
            continue
        nobs += 1
        old = mean
        mean += (x - old) / nobs
        m2 += (x - old) * (x - mean)
    if nobs < 2:
        return NAN
    return math.sqrt(m2 / (nobs - 1))


def _nansum(v: Sequence[float]) -> float:
    return float(sum(x for x in v if not math.isnan(x)))


def _nanrange(v: Sequence[float]) -> float:
    vals = [x for x in v if not math.isnan(x)]
    return (max(vals) - min(vals)) if vals else NAN


@dataclass
class _BinAccumulator:
    index: int
    bytes_in_flight: List[float] = field(default_factory=list)
    ack_rtt: List[float] = field(default_factory=list)
    packet_length: List[float] = field(default_factory=list)
    tcp_window_size: List[float] = field(default_factory=list)
    retransmission: List[float] = field(default_factory=list)
    pipefull: bool = False

    def row(self) -> np.ndarray:
        r = np.empty(len(FEATURE_CSV_COLUMNS), dtype=np.float64)
        r[COL["avg_bytes_in_flight"]] = _nanmean(self.bytes_in_flight)
        r[COL["stddev_bytes_in_flight"]] = _nanstd(self.bytes_in_flight)
        r[COL["sum_bytes_in_flight"]] = _nansum(self.bytes_in_flight)
        r[COL["avg_ack_rtt"]] = _nanmean(self.ack_rtt)
        r[COL["stddev_ack_rtt"]] = _nanstd(self.ack_rtt)
        r[COL["sum_ack_rtt"]] = _nansum(self.ack_rtt)
        r[COL["sum_packet_length"]] = _nansum(self.packet_length)
        r[COL["stddev_packet_length"]] = _nanstd(self.packet_length)
        r[COL["avg_packet_length"]] = _nanmean(self.packet_length)
        r[COL["avg_tcp_window_size"]] = _nanmean(self.tcp_window_size)
        r[COL["sum_retransmission"]] = _nanrange(self.retransmission)
        r[COL["sum_duplicate_ack"]] = 0.0
        r[COL["bbrpipefull"]] = 1.0 if self.pipefull else 0.0
        return np.array([_round_half_even(x) for x in r], dtype=np.float64)


class StreamingFeatureBuilder:
    """Turns a time-ordered stream of snapshots into 100 ms feature rows."""

    def __init__(self) -> None:
        self.start_ns: Optional[int] = None
        self._prev_acked: Optional[float] = None
        self._cur: Optional[_BinAccumulator] = None
        self.rows: List[np.ndarray] = []      # one row per closed, non-empty bin
        self.bin_index: List[int] = []         # time_bin of each row
        self.n_snapshots = 0
        self.late_samples = 0  # samples that arrived after their bin was closed by the clock

    def add(self, s: Snapshot) -> int:
        """Add one snapshot. Returns the number of rows closed by it (0 or 1)."""
        if self.start_ns is None:
            self.start_ns = s.ts_ns
        b = (s.ts_ns - self.start_ns) // BIN_NS
        if b < 0:
            b = 0  # out-of-order sample before the first one; never seen in practice
        if self.bin_index and b <= self.bin_index[-1]:
            # The bin was already closed by the clock (close_until) and a late
            # sample for it arrived. Bins cannot reopen; put it in the next one.
            # This makes the online features differ from offline: counted.
            b = self.bin_index[-1] + 1
            self.late_samples += 1
        closed = 0
        if self._cur is not None and b > self._cur.index:
            self._close()
            closed = 1
        if self._cur is None:
            self._cur = _BinAccumulator(index=b)

        # packet_length = BytesAcked.diff().fillna(0), negatives -> 0
        if self._prev_acked is None:
            delta = 0.0
        else:
            delta = s.bytes_acked - self._prev_acked
            if math.isnan(delta):
                delta = 0.0
            elif delta < 0:
                delta = 0.0
        self._prev_acked = s.bytes_acked

        c = self._cur
        c.bytes_in_flight.append(s.bytes_sent - s.bytes_acked - s.bytes_retrans)
        c.ack_rtt.append(s.rtt)
        c.packet_length.append(delta)
        c.tcp_window_size.append(s.snd_cwnd)
        c.retransmission.append(s.retransmits)
        if s.cwnd_gain == 512:
            c.pipefull = True
        self.n_snapshots += 1
        return closed

    def close_until(self, now_ns: int, margin_ns: int = 50_000_000) -> int:
        """Close the open bin if its 100 ms window ended more than margin_ns ago.

        Offline, a bin is final once its window has passed, even if the next
        snapshot comes much later (a stalled flow). Online, this lets the
        service act on such a bin without waiting for the next snapshot.
        """
        if self._cur is None or self.start_ns is None:
            return 0
        if now_ns >= self.start_ns + (self._cur.index + 1) * BIN_NS + margin_ns:
            self._close()
            return 1
        return 0

    def flush(self) -> int:
        """Close the open bin (end of flow)."""
        if self._cur is None:
            return 0
        self._close()
        return 1

    def _close(self) -> None:
        assert self._cur is not None
        self.rows.append(self._cur.row())
        self.bin_index.append(self._cur.index)
        self._cur = None

    @property
    def n_rows(self) -> int:
        return len(self.rows)

    def matrix(self) -> np.ndarray:
        """All closed rows, shape (n_rows, 13), FEATURE_CSV_COLUMNS order."""
        if not self.rows:
            return np.empty((0, len(FEATURE_CSV_COLUMNS)))
        return np.vstack(self.rows)


def features_from_dataframe(df) -> np.ndarray:
    """Load a *_features.csv DataFrame into the (n_rows, 13) matrix used here."""
    return df[FEATURE_CSV_COLUMNS].to_numpy(dtype=np.float64, na_value=np.nan)
