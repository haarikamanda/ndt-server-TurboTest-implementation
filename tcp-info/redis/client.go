// Package redis streams per-connection TCP snapshots to Redis for TurboTest.
//
// For every snapshot that tcp-info archives (the same records that end up in
// the .jsonl.zst files), the saver calls Client.AppendSnapshot. The client
// extracts the handful of tcp_info / BBR fields the TurboTest models use and
// RPUSHes one compact CSV row to "table_1:<uuid>":
//
//	ts_ns,BytesAcked,RTT,SndCwnd,BytesSent,BytesRetrans,Retransmits,CwndGain
//
// Empty fields mean "missing" (a record without TCPInfo or without BBRInfo),
// exactly like an empty cell in csvtool output. The TurboTest service does the
// 100 ms aggregation, so the bins match the offline training pipeline exactly.
//
// Writes never block the saver: rows go through a buffered channel to one
// writer goroutine that sends them in pipelined batches. If Redis is slow or
// down, rows are dropped and counted instead of stalling data collection.
package redis

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-lab/tcp-info/netlink"
	"github.com/m-lab/tcp-info/snapshot"
	"github.com/redis/go-redis/v9"
)

const (
	table1Prefix = "table_1:"
	// keyTTL bounds memory use: a download test lasts at most 15 s.
	keyTTL = 120 * time.Second
	// maxListLen keeps at most this many rows per connection (~60 s at 10 ms).
	maxListLen = 6000
	// batch limits for the writer goroutine.
	maxBatch      = 512
	flushInterval = 2 * time.Millisecond
	queueSize     = 65536
)

type row struct {
	key  string
	data string
}

// Client wraps a go-redis client and an asynchronous, batching writer.
type Client struct {
	rdb     *redis.Client
	ctx     context.Context
	cancel  context.CancelFunc
	queue   chan row
	wg      sync.WaitGroup
	dropped atomic.Int64
	written atomic.Int64
	// ports limits the output to sockets with these local ports (the ndt7
	// server ports); empty means every socket.
	ports map[uint16]bool
}

// NewClient returns a client for the Redis server at REDIS_ADDR. It returns
// (nil, nil) when REDIS_ADDR is not set, which disables the Redis output.
// REDIS_SRC_PORTS (comma-separated, e.g. "443,4443,8080") limits the output
// to sockets with these local ports, i.e. ndt7 tests on this server.
// A Redis server that is down at start is not an error: go-redis reconnects,
// and rows are dropped until it is reachable.
func NewClient(ctx context.Context) (*Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return nil, nil
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     4,
	})
	pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
	defer pcancel()
	if err := rdb.Ping(pctx).Err(); err != nil {
		log.Printf("redis: %s not reachable yet (%v); will keep retrying", addr, err)
	} else {
		log.Printf("redis: streaming snapshots to %s", addr)
	}
	c := newClient(ctx, rdb)
	ports, err := parsePorts(os.Getenv("REDIS_SRC_PORTS"))
	if err != nil {
		c.Close()
		return nil, err
	}
	c.ports = ports
	return c, nil
}

func parsePorts(s string) (map[uint16]bool, error) {
	ports := map[uint16]bool{}
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		p, err := strconv.ParseUint(f, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("bad REDIS_SRC_PORTS entry %q: %w", f, err)
		}
		ports[uint16(p)] = true
	}
	return ports, nil
}

// Wants reports whether snapshots of a socket with this local port are streamed.
func (c *Client) Wants(localPort uint16) bool {
	return len(c.ports) == 0 || c.ports[localPort]
}

func newClient(ctx context.Context, rdb *redis.Client) *Client {
	cctx, cancel := context.WithCancel(ctx)
	c := &Client{rdb: rdb, ctx: cctx, cancel: cancel, queue: make(chan row, queueSize)}
	c.wg.Add(1)
	go c.writer()
	return c
}

// FormatSnapshot returns the compact table_1 row for one archival record, or
// ok=false if the record cannot be decoded or has no timestamp.
func FormatSnapshot(rec *netlink.ArchivalRecord) (string, bool) {
	if rec == nil {
		return "", false
	}
	_, snap, err := snapshot.Decode(rec)
	if err != nil || snap == nil || snap.Timestamp.IsZero() {
		return "", false
	}
	b := make([]byte, 0, 96)
	b = strconv.AppendInt(b, snap.Timestamp.UnixNano(), 10)
	if t := snap.TCPInfo; t != nil {
		b = append(b, ',')
		b = strconv.AppendInt(b, t.BytesAcked, 10)
		b = append(b, ',')
		b = strconv.AppendUint(b, uint64(t.RTT), 10)
		b = append(b, ',')
		b = strconv.AppendUint(b, uint64(t.SndCwnd), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, t.BytesSent, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, t.BytesRetrans, 10)
		b = append(b, ',')
		b = strconv.AppendUint(b, uint64(t.Retransmits), 10)
	} else {
		b = append(b, ",,,,,,"...)
	}
	b = append(b, ',')
	if snap.BBRInfo != nil {
		b = strconv.AppendUint(b, uint64(snap.BBRInfo.CwndGain), 10)
	}
	return string(b), true
}

// AppendSnapshot queues one record for table_1:<uuid>. It never blocks.
func (c *Client) AppendSnapshot(uuid string, rec *netlink.ArchivalRecord) error {
	data, ok := FormatSnapshot(rec)
	if !ok {
		return fmt.Errorf("record for %s has no decodable snapshot", uuid)
	}
	select {
	case c.queue <- row{key: table1Prefix + uuid, data: data}:
		return nil
	default:
		c.dropped.Add(1)
		return fmt.Errorf("redis queue full; dropped snapshot for %s", uuid)
	}
}

func (c *Client) writer() {
	defer c.wg.Done()
	batch := make([]row, 0, maxBatch)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		pipe := c.rdb.Pipeline()
		seen := make(map[string]bool, 8)
		for _, r := range batch {
			pipe.RPush(context.Background(), r.key, r.data)
			seen[r.key] = true
		}
		for k := range seen {
			pipe.LTrim(context.Background(), k, -maxListLen, -1)
			pipe.Expire(context.Background(), k, keyTTL)
		}
		// Not c.ctx: the final flush runs after Close cancelled it.
		ectx, ecancel := context.WithTimeout(context.Background(), time.Second)
		_, err := pipe.Exec(ectx)
		ecancel()
		if err != nil {
			c.dropped.Add(int64(len(batch)))
			log.Printf("redis: dropped %d snapshots: %v", len(batch), err)
		} else {
			c.written.Add(int64(len(batch)))
		}
		batch = batch[:0]
	}
	for {
		select {
		case r := <-c.queue:
			batch = append(batch, r)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-c.ctx.Done():
			// Drain what is already queued, then stop.
			for {
				select {
				case r := <-c.queue:
					batch = append(batch, r)
				default:
					flush()
					return
				}
			}
		}
	}
}

// Stats returns the number of rows written and dropped so far.
func (c *Client) Stats() (written, dropped int64) {
	return c.written.Load(), c.dropped.Load()
}

// Close flushes queued rows and closes the connection.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.cancel()
	c.wg.Wait()
	return c.rdb.Close()
}
