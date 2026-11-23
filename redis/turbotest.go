// turbotest.go
// Job registration and results for the TurboTest inference service.
//
// Protocol (see turbotest/turbotest/service.py):
//
//	turbotest:queue          LIST   ndt-server RPUSHes {"uuid","epsilon","enqueued_ns"} at download start
//	table_1:<uuid>           LIST   tcp-info snapshots (written by tcp-info)
//	table_2:<uuid>           STRING 0/1 termination flag (0 set here, 1 set by the service)
//	turbotest:result:<uuid>  HASH   decision details (written by the service)
//	turbotest:done:<uuid>    STRING set here when the download ends

package redis

import (
	"context"
	"encoding/json"
	"time"
)

const (
	turboTestQueue  = "turbotest:queue"
	resultPrefix    = "turbotest:result:"
	donePrefix      = "turbotest:done:"
	turboTestKeyTTL = 5 * time.Minute
)

// maxQueueLen bounds turbotest:queue if the service is down; older jobs are
// dropped (those tests simply run to full length).
const maxQueueLen = 10000

type turboTestJob struct {
	UUID    string `json:"uuid"`
	Epsilon int    `json:"epsilon"`
	// EnqueuedNs lets the service drop jobs that waited too long (e.g.
	// queued while the service was down).
	EnqueuedNs int64 `json:"enqueued_ns"`
}

// Ping checks the connection.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// RegisterTurboTest sets table_2:<uuid> = 0 and queues the test for the
// TurboTest service.
func (c *Client) RegisterTurboTest(ctx context.Context, uuid string, epsilon int) error {
	job, err := json.Marshal(turboTestJob{UUID: uuid, Epsilon: epsilon, EnqueuedNs: time.Now().UnixNano()})
	if err != nil {
		return err
	}
	pipe := c.rdb.TxPipeline()
	pipe.Set(ctx, table2Prefix+uuid, 0, turboTestKeyTTL)
	pipe.Del(ctx, resultPrefix+uuid, donePrefix+uuid)
	pipe.RPush(ctx, turboTestQueue, job)
	pipe.LTrim(ctx, turboTestQueue, -maxQueueLen, -1)
	_, err = pipe.Exec(ctx)
	return err
}

// GetTerminationFlags reads table_2 for many tests in one round trip.
// The result has one entry per uuid: true if the flag is "1".
func (c *Client) GetTerminationFlags(ctx context.Context, uuids []string) ([]bool, error) {
	keys := make([]string, len(uuids))
	for i, u := range uuids {
		keys[i] = table2Prefix + u
	}
	vals, err := c.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(uuids))
	for i, v := range vals {
		if s, ok := v.(string); ok && s == "1" {
			out[i] = true
		}
	}
	return out, nil
}

// MarkTurboTestDone tells the service that the download has ended.
func (c *Client) MarkTurboTestDone(ctx context.Context, uuid string) error {
	return c.rdb.Set(ctx, donePrefix+uuid, 1, turboTestKeyTTL).Err()
}

// GetTurboTestResult returns the service's decision fields (empty if none yet).
func (c *Client) GetTurboTestResult(ctx context.Context, uuid string) (map[string]string, error) {
	return c.rdb.HGetAll(ctx, resultPrefix+uuid).Result()
}
