package handler

import (
	"context"
	"encoding/json"
	"fmt"
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
		{"turbotest=true", &turboTestParams{15}, false},
		{"turbotest=1&turbotest_epsilon=35", &turboTestParams{35}, false},
		{"turbotest=false", nil, false},
		{"turbotest=on", nil, true},
		{"turbotest=true&turbotest_epsilon=7", nil, true},
		{"turbotest=true&turbotest_epsilon=x", nil, true},
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

// The service's flag closes the sender's stop channel and the
// decision fields reach the archived result.
func Test_turboTestStop(t *testing.T) {
	h, mr := newTestHandler(t)
	sp := &sender.Params{}
	run := h.startTurboTest(context.Background(), "u1", &turboTestParams{20}, sp)
	if run == nil || sp.TurboTestStop == nil || sp.TurboTestResult == nil {
		t.Fatal("TurboTest must wire the sender")
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
	if run.result().TerminatedEarly {
		t.Fatal("TerminatedEarly must be set only when the sender stops")
	}
	res := sp.TurboTestResult() // what the sender calls when it stops
	if res.Decision != "stop" || res.Tau != 15 || res.PredictedMbps != 93.5 || !res.TerminatedEarly {
		t.Fatalf("bad result %+v", res)
	}
	final := run.finish()
	if final.Epsilon != 20 {
		t.Fatalf("bad final %+v", final)
	}
	if !mr.Exists("turbotest:done:u1") {
		t.Fatal("done marker not set")
	}
}

// No decision: the archive records the service's no_stop, or "none".
func Test_turboTestNoDecision(t *testing.T) {
	h, mr := newTestHandler(t)
	run := h.startTurboTest(context.Background(), "u3", &turboTestParams{15}, &sender.Params{})
	mr.HSet("turbotest:result:u3", "status", "no_stop")
	if res := run.finish(); res.Decision != "no_stop" || res.TerminatedEarly {
		t.Fatalf("bad result %+v", res)
	}
	h2 := &Handler{}
	if h2.startTurboTest(context.Background(), "u4", &turboTestParams{15}, &sender.Params{}) != nil {
		t.Fatal("without Redis, startTurboTest must return nil")
	}
}

// A failing Redis opens the breaker: further tests skip TurboTest at once.
func Test_turboTestBreaker(t *testing.T) {
	h, mr := newTestHandler(t)
	mr.Close()
	for i := 0; i < breakerThreshold; i++ {
		if h.startTurboTest(context.Background(), "x", &turboTestParams{15}, &sender.Params{}) != nil {
			t.Fatal("register must fail with Redis down")
		}
	}
	t0 := time.Now()
	if h.startTurboTest(context.Background(), "y", &turboTestParams{15}, &sender.Params{}) != nil {
		t.Fatal("breaker should be open")
	}
	if time.Since(t0) > 50*time.Millisecond {
		t.Fatal("open breaker must not wait on Redis")
	}
}

// Many concurrent downloads share one poller; each sees only its own flag.
func Test_turboTestSharedPoller(t *testing.T) {
	h, mr := newTestHandler(t)
	var runs []*turboTestRun
	var params []*sender.Params
	for i := 0; i < 50; i++ {
		sp := &sender.Params{}
		runs = append(runs, h.startTurboTest(context.Background(), fmt.Sprintf("c%d", i), &turboTestParams{15}, sp))
		params = append(params, sp)
	}
	mr.Set("table_2:c7", "1")
	select {
	case <-params[7].TurboTestStop:
	case <-time.After(2 * time.Second):
		t.Fatal("c7 not stopped")
	}
	select {
	case <-params[8].TurboTestStop:
		t.Fatal("c8 stopped without a flag")
	case <-time.After(50 * time.Millisecond):
	}
	for _, r := range runs {
		r.finish()
	}
	if n := len(h.poller().waiters); n != 0 {
		t.Fatalf("%d waiters leaked", n)
	}
}
