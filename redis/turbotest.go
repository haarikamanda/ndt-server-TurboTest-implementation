// turbotest.go
// Job registration and results for the TurboTest inference service.
//
// Protocol (see turbotest/turbotest/service.py):
//
//	turbotest:queue          LIST   ndt-server RPUSHes {"uuid","mode","epsilon"} at download start
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

type turboTestJob struct {
	UUID    string `json:"uuid"`
	Mode    string `json:"mode"`
	Epsilon int    `json:"epsilon"`
}

// Ping checks the connection.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// RegisterTurboTest sets table_2:<uuid> = 0 and queues the test for the
// TurboTest service.
func (c *Client) RegisterTurboTest(ctx context.Context, uuid, mode string, epsilon int) error {
	job, err := json.Marshal(turboTestJob{UUID: uuid, Mode: mode, Epsilon: epsilon})
	if err != nil {
		return err
	}
	pipe := c.rdb.TxPipeline()
	pipe.Set(ctx, table2Prefix+uuid, 0, turboTestKeyTTL)
	pipe.Del(ctx, resultPrefix+uuid, donePrefix+uuid)
	pipe.RPush(ctx, turboTestQueue, job)
	_, err = pipe.Exec(ctx)
	return err
}

// MarkTurboTestDone tells the service that the download has ended.
func (c *Client) MarkTurboTestDone(ctx context.Context, uuid string) error {
	return c.rdb.Set(ctx, donePrefix+uuid, 1, turboTestKeyTTL).Err()
}

// GetTurboTestResult returns the service's decision fields (empty if none yet).
func (c *Client) GetTurboTestResult(ctx context.Context, uuid string) (map[string]string, error) {
	return c.rdb.HGetAll(ctx, resultPrefix+uuid).Result()
}
