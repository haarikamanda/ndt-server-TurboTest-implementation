package handler

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/m-lab/ndt-server/ndt7/download/sender"
	"github.com/m-lab/ndt-server/redis"
)

func Test_validateTurboTest(t *testing.T) {
	tests := []struct {
		q       string
		want    *turboTestParams
		wantErr bool
	}{
		{"", nil, false},
		{"turbotest=enforce", &turboTestParams{"enforce", 15}, false},
		{"turbotest=shadow&turbotest_epsilon=35", &turboTestParams{"shadow", 35}, false},
		{"turbotest=on", nil, true},
		{"turbotest=enforce&turbotest_epsilon=7", nil, true},
		{"turbotest=enforce&turbotest_epsilon=x", nil, true},
	}
	for _, tt := range tests {
		v, _ := url.ParseQuery(tt.q)
		got, err := validateTurboTest(v)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q: err=%v", tt.q, err)
		}
		if (got == nil) != (tt.want == nil) || (got != nil && *got != *tt.want) {
			t.Errorf("%q: got %+v want %+v", tt.q, got, tt.want)
		}
	}
}

func newTestHandler(t *testing.T) (*Handler, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	return &Handler{RedisClient: redis.NewClient(mr.Addr()), TurboTestPollInterval: 5 * time.Millisecond}, mr
}

// Enforce: the service's flag closes the sender's stop channel and the
// decision fields reach the archived result.
func Test_turboTestEnforce(t *testing.T) {
	h, mr := newTestHandler(t)
	sp := &sender.Params{}
	run := h.startTurboTest(context.Background(), "u1", &turboTestParams{"enforce", 20}, sp)
	if run == nil || sp.TurboTestStop == nil || sp.TurboTestResult == nil {
		t.Fatal("enforce mode must wire the sender")
	}
	if v, _ := mr.Get("table_2:u1"); v != "0" {
		t.Fatalf("table_2 = %q, want 0", v)
	}
	job, _ := mr.Lpop("turbotest:queue")
	var j map[string]any
	if err := json.Unmarshal([]byte(job), &j); err != nil || j["uuid"] != "u1" || j["epsilon"].(float64) != 20 {
		t.Fatalf("bad job %q", job)
	}
	// The service decides.
	mr.HSet("turbotest:result:u1", "status", "stop", "tau", "15", "predicted_mbps", "93.5",
		"probability", "0.81", "stop_after_ms", "1500.0", "decided_at_ns", "1")
	mr.Set("table_2:u1", "1")
	select {
	case <-sp.TurboTestStop:
	case <-time.After(2 * time.Second):
		t.Fatal("stop channel not closed")
	}
	res := sp.TurboTestResult()
	if res.Decision != "stop" || res.Tau != 15 || res.PredictedMbps != 93.5 || !res.TerminatedEarly {
		t.Fatalf("bad result %+v", res)
	}
	final := run.finish()
	if final.Epsilon != 20 || final.Mode != "enforce" {
		t.Fatalf("bad final %+v", final)
	}
	if !mr.Exists("turbotest:done:u1") {
		t.Fatal("done marker not set")
	}
}

// Shadow: the decision is recorded but the sender is never stopped.
func Test_turboTestShadow(t *testing.T) {
	h, mr := newTestHandler(t)
	sp := &sender.Params{}
	run := h.startTurboTest(context.Background(), "u2", &turboTestParams{"shadow", 15}, sp)
	if run == nil || sp.TurboTestStop != nil {
		t.Fatal("shadow mode must not wire the sender stop channel")
	}
	mr.HSet("turbotest:result:u2", "status", "stop", "tau", "10", "predicted_mbps", "12")
	mr.Set("table_2:u2", "1")
	time.Sleep(100 * time.Millisecond)
	res := run.finish()
	if res.Decision != "stop" || res.Tau != 10 || res.TerminatedEarly || res.FlagSeenMs <= 0 {
		t.Fatalf("bad shadow result %+v", res)
	}
}

// No decision: the archive records the service's no_stop, or "none".
func Test_turboTestNoDecision(t *testing.T) {
	h, mr := newTestHandler(t)
	run := h.startTurboTest(context.Background(), "u3", &turboTestParams{"enforce", 15}, &sender.Params{})
	mr.HSet("turbotest:result:u3", "status", "no_stop")
	if res := run.finish(); res.Decision != "no_stop" || res.TerminatedEarly {
		t.Fatalf("bad result %+v", res)
	}
	h2 := &Handler{}
	if h2.startTurboTest(context.Background(), "u4", &turboTestParams{"enforce", 15}, &sender.Params{}) != nil {
		t.Fatal("without Redis, startTurboTest must return nil")
	}
}
