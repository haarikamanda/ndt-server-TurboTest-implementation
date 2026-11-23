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
	"github.com/m-lab/ndt-server/redis"
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

// turboTestPoller is shared by all downloads of one server: every
// interval it reads the table_2 flags of all active TurboTest downloads with
// one MGET, instead of one GET loop per test. It also holds a circuit
// breaker so a failing Redis does not delay every test.
type turboTestPoller struct {
	rc       *redis.Client
	interval time.Duration

	mu        sync.Mutex
	started   bool
	waiters   map[string]chan struct{}
	failures  int
	openUntil time.Time
}

const (
	breakerThreshold = 5                // consecutive Redis errors to open the breaker
	breakerCooldown  = 10 * time.Second // TurboTest skipped while open
	registerTimeout  = 200 * time.Millisecond
	finishBudget     = 300 * time.Millisecond
)

var pollersMu sync.Mutex

func (h *Handler) poller() *turboTestPoller {
	pollersMu.Lock()
	defer pollersMu.Unlock()
	if h.turboTest == nil {
		h.turboTest = &turboTestPoller{rc: h.RedisClient, interval: h.pollInterval(),
			waiters: map[string]chan struct{}{}}
	}
	return h.turboTest
}

// allow reports whether TurboTest should be tried (breaker closed).
func (p *turboTestPoller) allow() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Now().After(p.openUntil)
}

func (p *turboTestPoller) report(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil {
		p.failures = 0
		return
	}
	p.failures++
	if p.failures >= breakerThreshold {
		p.openUntil = time.Now().Add(breakerCooldown)
		p.failures = 0
		logging.Logger.WithError(err).Warn("turbotest: Redis failing; TurboTest disabled for 10s")
	}
}

func (p *turboTestPoller) add(uuid string) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := make(chan struct{})
	p.waiters[uuid] = ch
	if !p.started {
		p.started = true
		go p.loop()
	}
	return ch
}

func (p *turboTestPoller) remove(uuid string) {
	p.mu.Lock()
	delete(p.waiters, uuid)
	p.mu.Unlock()
}

func (p *turboTestPoller) loop() {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for range t.C {
		p.mu.Lock()
		uuids := make([]string, 0, len(p.waiters))
		for u := range p.waiters {
			uuids = append(uuids, u)
		}
		p.mu.Unlock()
		if len(uuids) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		flags, err := p.rc.GetTerminationFlags(ctx, uuids)
		cancel()
		p.report(err)
		if err != nil {
			continue
		}
		p.mu.Lock()
		for i, set := range flags {
			if !set {
				continue
			}
			if ch, ok := p.waiters[uuids[i]]; ok {
				close(ch)
				delete(p.waiters, uuids[i])
			}
		}
		p.mu.Unlock()
	}
}

// turboTestRun follows one download: it registers the test with the
// TurboTest service, waits for its table_2 flag (via the shared poller),
// and (in enforce mode) tells the sender to stop.
type turboTestRun struct {
	h      *Handler
	p      *turboTestPoller
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
// TurboTest is not available (no Redis, Redis error, breaker open): the test
// then runs normally.
func (h *Handler) startTurboTest(ctx context.Context, uuid string, p *turboTestParams, sp *sender.Params) *turboTestRun {
	if h.RedisClient == nil {
		logging.Logger.Warn("turbotest requested but no Redis client is configured")
		return nil
	}
	pl := h.poller()
	if !pl.allow() {
		return nil
	}
	rctx, rcancel := context.WithTimeout(ctx, registerTimeout)
	err := h.RedisClient.RegisterTurboTest(rctx, uuid, p.Mode, p.Epsilon)
	rcancel()
	pl.report(err)
	if err != nil {
		logging.Logger.WithError(err).Warn("turbotest: register failed; running full test")
		return nil
	}
	wctx, wcancel := context.WithCancel(ctx)
	run := &turboTestRun{
		h: h, p: pl, uuid: uuid, start: time.Now(), stop: make(chan struct{}), cancel: wcancel,
		done: make(chan struct{}),
		res:  model.TurboTest{Mode: p.Mode, Epsilon: p.Epsilon, Decision: "none"},
	}
	if p.Mode == "enforce" {
		sp.TurboTestStop = run.stop
		sp.TurboTestResult = run.senderStopped
	}
	go run.watch(wctx, pl.add(uuid))
	return run
}

func (r *turboTestRun) result() *model.TurboTest {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.res
	return &c
}

// senderStopped is called by the sender when it ends the download because
// of TurboTest; only then is the test marked as terminated early.
func (r *turboTestRun) senderStopped() *model.TurboTest {
	r.mu.Lock()
	r.res.TerminatedEarly = true
	r.mu.Unlock()
	return r.result()
}

func (r *turboTestRun) watch(ctx context.Context, flag <-chan struct{}) {
	defer close(r.done)
	select {
	case <-ctx.Done():
		r.p.remove(r.uuid)
		return
	case <-flag:
	}
	seen := time.Now()
	r.mu.Lock()
	r.res.FlagSeenMs = float64(seen.Sub(r.start).Microseconds()) / 1e3
	enforce := r.res.Mode == "enforce"
	r.mu.Unlock()
	rctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	r.readResult(rctx, seen)
	cancel()
	logging.Logger.Infof("turbotest: %s stop flag seen after %.0f ms", r.uuid, float64(seen.Sub(r.start).Milliseconds()))
	if enforce {
		close(r.stop)
	}
}

// readResult copies the service's decision fields into r.res.
func (r *turboTestRun) readResult(ctx context.Context, seen time.Time) error {
	m, err := r.h.RedisClient.GetTurboTestResult(ctx, r.uuid)
	if err != nil || len(m) == 0 {
		return err
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
	return nil
}

// finish stops the watcher, tells the service the test ended, and returns
// the decision to archive. It spends at most finishBudget on Redis.
func (r *turboTestRun) finish() *model.TurboTest {
	r.cancel()
	<-r.done
	ctx, cancel := context.WithTimeout(context.Background(), finishBudget)
	defer cancel()
	err := r.h.RedisClient.MarkTurboTestDone(ctx, r.uuid)
	r.p.report(err)
	if err != nil {
		logging.Logger.WithError(err).Warn("turbotest: mark done failed")
		return r.result()
	}
	// The service writes "no_stop" once it sees the done marker. Wait
	// briefly for it so the archive says why the test was not stopped.
	for r.result().Decision == "none" && ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
			if r.readResult(ctx, time.Time{}) != nil {
				return r.result()
			}
		}
	}
	return r.result()
}
