package model

// TurboTest describes the TurboTest early-termination decision for one
// download subtest. It is sent to the client in the final measurement of an
// enforced early stop, and saved with the download's archival data.
type TurboTest struct {
	// Mode is "enforce" (stop the test) or "shadow" (record only; run full length).
	Mode string
	// Epsilon selects the Stage 2 classifier (the paper's accuracy tolerance).
	Epsilon int
	// Decision is "stop", "no_stop" (the classifier never said stop), or
	// "none" (no answer from the TurboTest service before the test ended).
	Decision string
	// Tau is the number of 100 ms tcp-info rows seen when the classifier said stop.
	Tau int `json:",omitempty"`
	// PredictedMbps is the Stage 1 throughput prediction at Tau (Mbit/s).
	// This is the TurboTest test result when the test stops early.
	PredictedMbps float64 `json:",omitempty"`
	// Probability is the Stage 2 classifier output at Tau.
	Probability float64 `json:",omitempty"`
	// StopAfterMs is the end of the Tau-th row, relative to the flow's first
	// tcp-info snapshot (as computed by the TurboTest service).
	StopAfterMs float64 `json:",omitempty"`
	// FlagSeenMs is when ndt-server saw table_2 = 1, relative to the start of
	// the download (sender start).
	FlagSeenMs float64 `json:",omitempty"`
	// FlagLatencyMs is the time from the service writing the decision to
	// ndt-server seeing it.
	FlagLatencyMs float64 `json:",omitempty"`
	// TerminatedEarly is true if ndt-server ended the download because of TurboTest.
	TerminatedEarly bool
}
