"""Helpers to read csvtool output (tcp-info .jsonl.zst -> CSV) as TurboTest snapshots."""

import numpy as np
import pandas as pd

from turbotest.features import Snapshot

NEEDED = ["Timestamp", "TCP.BytesAcked", "TCP.RTT", "TCP.SndCwnd", "TCP.BytesSent",
          "TCP.BytesRetrans", "TCP.Retransmits", "BBR.CwndGain"]


def read_csvtool(path):
    """Read a csvtool CSV with only the columns the features need."""
    head = pd.read_csv(path, nrows=0).columns
    return pd.read_csv(path, usecols=[c for c in NEEDED if c in head], low_memory=False)


def snapshots(df):
    """Rows -> Snapshot list, after the same row filtering as the training script."""
    df = df.iloc[1:].reset_index(drop=True)          # metadata placeholder row
    df = df.dropna(subset=["Timestamp"])
    ts = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df[ts.notna()]
    ts = ts[ts.notna()]
    num = lambda c: pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=float) if c in df else np.full(len(df), np.nan)  # noqa: E731
    cols = [num(c) for c in NEEDED[1:]]
    return [Snapshot(int(t.value), *(float(c[i]) for c in cols)) for i, t in enumerate(ts)]


def reference_features(df):
    """Verbatim logic of model_shared/preprocessing_scripts/2_features_extraction_final.py
    (process_csv without file IO). Returns the per-bin DataFrame."""
    df = df.iloc[1:].reset_index(drop=True)
    df = df.dropna(subset=['Timestamp'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df.dropna(subset=['Timestamp'])
    start_time = df['Timestamp'].min()
    df['timestamp_ms'] = (df['Timestamp'] - start_time).dt.total_seconds() * 1000
    df['time_bin'] = (df['timestamp_ms'] // 100).astype(int)
    cols = ['TCP.BytesAcked', 'TCP.RTT', 'TCP.SndCwnd', 'TCP.BytesSent', 'TCP.BytesRetrans', 'TCP.Retransmits']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df['bytes_in_flight'] = df['TCP.BytesSent'] - df['TCP.BytesAcked'] - df['TCP.BytesRetrans']
    df['ack_rtt'] = df['TCP.RTT']
    df['tcp_window_size'] = df['TCP.SndCwnd']
    df['retransmission'] = df['TCP.Retransmits']
    df['bytes_delta'] = df['TCP.BytesAcked'].diff().fillna(0)
    df.loc[df['bytes_delta'] < 0, 'bytes_delta'] = 0
    df['packet_length'] = df['bytes_delta']
    df['BBR.CwndGain'] = pd.to_numeric(df.get('BBR.CwndGain'), errors='coerce')
    df['is_pipefull'] = df['BBR.CwndGain'] == 512
    agg_df = df.groupby('time_bin').agg(
        time_bin=('time_bin', 'first'),
        avg_bytes_in_flight=('bytes_in_flight', 'mean'),
        stddev_bytes_in_flight=('bytes_in_flight', 'std'),
        sum_bytes_in_flight=('bytes_in_flight', 'sum'),
        avg_ack_rtt=('ack_rtt', 'mean'),
        stddev_ack_rtt=('ack_rtt', 'std'),
        sum_ack_rtt=('ack_rtt', 'sum'),
        sum_packet_length=('packet_length', 'sum'),
        stddev_packet_length=('packet_length', 'std'),
        avg_packet_length=('packet_length', 'mean'),
        avg_tcp_window_size=('tcp_window_size', 'mean'),
        sum_retransmission=('retransmission', lambda x: x.max() - x.min()),
        sum_duplicate_ack=('retransmission', lambda x: 0),
        bbrpipefull=('is_pipefull', 'any')
    ).reset_index(drop=True)
    agg_df['bbrpipefull'] = agg_df['bbrpipefull'].astype(int)
    agg_df = agg_df.round(0).astype('Int64')
    return agg_df
