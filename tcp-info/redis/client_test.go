package redis

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/m-lab/tcp-info/netlink"
	"github.com/m-lab/tcp-info/snapshot"
	"github.com/m-lab/tcp-info/zstd"
	"github.com/redis/go-redis/v9"
)

const recording = "../snapshot/testdata/ndt-jdczh_1553815964_00000000000003E8.00185.jsonl.zst"

func readRecords(t *testing.T) []*netlink.ArchivalRecord {
	t.Helper()
	rdr := zstd.NewReader(recording)
	defer rdr.Close()
	ar := netlink.NewArchiveReader(rdr)
	var recs []*netlink.ArchivalRecord
	for {
		rec, err := ar.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, rec)
	}
	return recs
}

// The row must carry exactly the csvtool fields the TurboTest features use.
func TestFormatSnapshotMatchesDecode(t *testing.T) {
	n := 0
	for _, rec := range readRecords(t) {
		_, snap, err := snapshot.Decode(rec)
		row, ok := FormatSnapshot(rec)
		if err != nil || snap == nil || snap.Timestamp.IsZero() {
			if ok {
				t.Fatalf("metadata/undecodable record produced a row: %q", row)
			}
			continue
		}
		if !ok {
			t.Fatal("decodable record produced no row")
		}
		f := strings.Split(row, ",")
		if len(f) != 8 {
			t.Fatalf("want 8 fields, got %d: %q", len(f), row)
		}
		if f[0] != strconv.FormatInt(snap.Timestamp.UnixNano(), 10) {
			t.Errorf("timestamp %s != %d", f[0], snap.Timestamp.UnixNano())
		}
		if ti := snap.TCPInfo; ti != nil {
			want := []string{
				strconv.FormatInt(ti.BytesAcked, 10), strconv.FormatUint(uint64(ti.RTT), 10),
				strconv.FormatUint(uint64(ti.SndCwnd), 10), strconv.FormatInt(ti.BytesSent, 10),
				strconv.FormatInt(ti.BytesRetrans, 10), strconv.FormatUint(uint64(ti.Retransmits), 10),
			}
			for i, w := range want {
				if f[i+1] != w {
					t.Errorf("field %d = %s, want %s", i+1, f[i+1], w)
				}
			}
		}
		if snap.BBRInfo == nil && f[7] != "" {
			t.Errorf("no BBRInfo but CwndGain=%q", f[7])
		}
		n++
	}
	if n < 10 {
		t.Fatalf("only %d snapshots decoded", n)
	}
}

func TestAppendSnapshotWritesAllRows(t *testing.T) {
	mr := miniredis.RunT(t)
	c := newClient(context.Background(), redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	want := 0
	for _, rec := range readRecords(t) {
		if _, ok := FormatSnapshot(rec); ok {
			if err := c.AppendSnapshot("uuid-1", rec); err != nil {
				t.Fatal(err)
			}
			want++
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := mr.List("table_1:uuid-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != want {
		t.Fatalf("got %d rows, want %d", len(got), want)
	}
	if ttl := mr.TTL("table_1:uuid-1"); ttl <= 0 || ttl > keyTTL {
		t.Errorf("bad TTL %v", ttl)
	}
	w, d := c.Stats()
	if w != int64(want) || d != 0 {
		t.Errorf("stats written=%d dropped=%d", w, d)
	}
}

func TestNewClientDisabledWithoutAddr(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	c, err := NewClient(context.Background())
	if c != nil || err != nil {
		t.Fatalf("want (nil, nil), got (%v, %v)", c, err)
	}
}

func TestPortsFilter(t *testing.T) {
	p, err := parsePorts(" 443, 8080 ,")
	if err != nil || len(p) != 2 || !p[443] || !p[8080] {
		t.Fatalf("got %v %v", p, err)
	}
	if _, err := parsePorts("x"); err == nil {
		t.Fatal("want error")
	}
	c := &Client{ports: p}
	if !c.Wants(443) || c.Wants(6379) {
		t.Fatal("filter wrong")
	}
	if !(&Client{}).Wants(6379) {
		t.Fatal("empty filter must accept all")
	}
}
