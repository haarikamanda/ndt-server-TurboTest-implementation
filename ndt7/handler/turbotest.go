package handler

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/m-lab/ndt-server/logging"
	"github.com/m-lab/ndt-server/ndt7/download/sender"
	"github.com/m-lab/ndt-server/ndt7/model"
	"github.com/m-lab/ndt-server/ndt7/spec"
)

// DefaultTurboTestPollInterval is how often ndt-server reads table_2:<uuid>.
// The design doc uses 100 ms; a shorter interval costs one tiny GET and stops
// the test sooner after the decision.
const DefaultTurboTestPollInterval = 10 * time.Millisecond

type turboTestParams struct {
	Mode    string
	Epsilon int
}

// validateTurboTest returns nil (and no error) if the client did not ask for TurboTest.
func validateTurboTest(values url.Values) (*turboTestParams, error) {
	mode := values.Get(spec.TurboTestParameterName)
	if mode == "" {
		return nil, nil
	}
	if !slices.Contains(spec.ValidTurboTestModes, mode) {
		return nil, fmt.Errorf("invalid %s parameter value %s", spec.TurboTestParameterName, mode)
	}
	eps := spec.DefaultTurboTestEpsilon
	if v := values.Get(spec.TurboTestEpsilonParameterName); v != "" {
		e, err := strconv.Atoi(v)
		if err != nil || !slices.Contains(spec.ValidTurboTestEpsilons, e) {
			return nil, fmt.Errorf("invalid %s parameter value %s", spec.TurboTestEpsilonParameterName, v)
		}
		eps = e
	}
	return &turboTestParams{Mode: mode, Epsilon: eps}, nil
}

// turboTestRun follows one download: it registers the test with the
// TurboTest service, polls table_2:<uuid>, and (in enforce mode) tells the
// sender to stop.
type turboTestRun struct {
	h      *Handler
	uuid   string
	start  time.Time
	stop   chan struct{}
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	res model.TurboTest
}

func (h *Handler) pollInterval() time.Duration {
	if h.TurboTestPollInterval > 0 {
		return h.TurboTestPollInterval
	}
	return DefaultTurboTestPollInterval
}

// startTurboTest registers the test and starts the watcher. It returns nil if
// TurboTest is not available (no Redis, or Redis error): the test then runs
// normally.
func (h *Handler) startTurboTest(ctx context.Context, uuid string, p *turboTestParams, sp *sender.Params) *turboTestRun {
	if h.RedisClient == nil {
		logging.Logger.Warn("turbotest requested but no Redis client is configured")
		return nil
	}
	rctx, rcancel := context.WithTimeout(ctx, 500*time.Millisecond)
	err := h.RedisClient.RegisterTurboTest(rctx, uuid, p.Mode, p.Epsilon)
	rcancel()
	if err != nil {
		logging.Logger.WithError(err).Warn("turbotest: register failed; running full test")
		return nil
	}
	wctx, wcancel := context.WithCancel(ctx)
	run := &turboTestRun{
		h: h, uuid: uuid, start: time.Now(), stop: make(chan struct{}), cancel: wcancel,
		done: make(chan struct{}),
		res:  model.TurboTest{Mode: p.Mode, Epsilon: p.Epsilon, Decision: "none"},
	}
	if p.Mode == "enforce" {
		sp.TurboTestStop = run.stop
		sp.TurboTestResult = run.result
	}
	go run.watch(wctx)
	return run
}

func (r *turboTestRun) result() *model.TurboTest {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.res
	return &c
}

func (r *turboTestRun) watch(ctx context.Context) {
	defer close(r.done)
	t := time.NewTicker(r.h.pollInterval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		flag, err := r.h.RedisClient.GetTerminationFlag(ctx, r.uuid)
		if err != nil || flag != 1 {
			continue
		}
		seen := time.Now()
		r.mu.Lock()
		r.res.FlagSeenMs = float64(seen.Sub(r.start).Microseconds()) / 1e3
		r.mu.Unlock()
		r.readResult(ctx, seen)
		r.mu.Lock()
		enforce := r.res.Mode == "enforce"
		r.res.TerminatedEarly = enforce
		r.mu.Unlock()
		logging.Logger.Infof("turbotest: %s stop flag seen after %.0f ms", r.uuid, float64(seen.Sub(r.start).Milliseconds()))
		if enforce {
			close(r.stop)
		}
		return
	}
}

// readResult copies the service's decision fields into r.res.
func (r *turboTestRun) readResult(ctx context.Context, seen time.Time) {
	rctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	m, err := r.h.RedisClient.GetTurboTestResult(rctx, r.uuid)
	if err != nil || len(m) == 0 {
		return
	}
	f := func(k string) float64 {
		v, _ := strconv.ParseFloat(m[k], 64)
		return v
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if s := m["status"]; s != "" {
		r.res.Decision = s
	}
	r.res.Tau = int(f("tau"))
	r.res.PredictedMbps = f("predicted_mbps")
	r.res.Probability = f("probability")
	r.res.StopAfterMs = f("stop_after_ms")
	if ns, err := strconv.ParseInt(m["decided_at_ns"], 10, 64); err == nil && ns > 0 && !seen.IsZero() {
		r.res.FlagLatencyMs = float64(seen.UnixNano()-ns) / 1e6
	}
}

// finish stops the watcher, tells the service the test ended, and returns
// the decision to archive.
func (r *turboTestRun) finish() *model.TurboTest {
	r.cancel()
	<-r.done
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := r.h.RedisClient.MarkTurboTestDone(ctx, r.uuid); err != nil {
		logging.Logger.WithError(err).Warn("turbotest: mark done failed")
	}
	if r.result().Decision == "none" {
		// The service writes "no_stop" once it sees the done marker. Wait
		// briefly for it so the archive says why the test was not stopped.
		for i := 0; i < 10 && r.result().Decision == "none"; i++ {
			time.Sleep(20 * time.Millisecond)
			r.readResult(ctx, time.Time{})
		}
	}
	return r.result()
}
