"""Streaming features must equal the offline training preprocessing exactly."""

import glob
import os
import sys

import numpy as np
import pandas as pd
import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, ".."))

from tools.tcpinfo_csv import reference_features, snapshots  # noqa: E402
from turbotest.features import FEATURE_CSV_COLUMNS, Snapshot, StreamingFeatureBuilder  # noqa: E402

FIXTURES = sorted(glob.glob(os.path.join(HERE, "data", "*.csv.gz")))


@pytest.mark.parametrize("path", FIXTURES, ids=[os.path.basename(p)[:40] for p in FIXTURES])
def test_streaming_equals_offline(path):
    df = pd.read_csv(path, low_memory=False)
    ref = reference_features(df.copy())
    b = StreamingFeatureBuilder()
    for s in snapshots(df):
        b.add(s)
    b.flush()
    got = b.matrix()
    want = ref[FEATURE_CSV_COLUMNS].to_numpy(dtype=float, na_value=np.nan)
    assert got.shape == want.shape
    assert b.bin_index == ref["time_bin"].astype(int).tolist()
    np.testing.assert_array_equal(got, want)  # NaN == NaN for assert_array_equal


def test_snapshot_parse_roundtrip():
    s = Snapshot.parse("1700000000123456789,100,2000,10,150,5,1,512")
    assert s.ts_ns == 1700000000123456789 and s.cwnd_gain == 512 and s.bytes_acked == 100
    s = Snapshot.parse("1700000000123456789,,,,,,,")
    assert np.isnan(s.bytes_acked) and np.isnan(s.cwnd_gain)


def test_clock_close_and_late_sample():
    b = StreamingFeatureBuilder()
    t0 = 1_000_000_000_000
    b.add(Snapshot(t0, 0, 1, 1, 0, 0, 0))
    b.add(Snapshot(t0 + 50_000_000, 1000, 1, 1, 1000, 0, 0))
    assert b.close_until(t0 + 120_000_000) == 0          # within margin
    assert b.close_until(t0 + 151_000_000) == 1          # window + 50 ms passed
    assert b.n_rows == 1 and b.rows[0][FEATURE_CSV_COLUMNS.index("sum_packet_length")] == 1000
    b.add(Snapshot(t0 + 90_000_000, 2000, 1, 1, 2000, 0, 0))  # late sample for bin 0
    assert b.flush() == 1 and b.bin_index == [0, 1]
