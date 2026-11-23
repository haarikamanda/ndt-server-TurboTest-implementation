package spec

// TurboTest is the early-termination decision that a TurboTest-enabled
// ndt-server sends in the last measurement of a download it stopped early.
type TurboTest struct {
	// Mode is "enforce" or "shadow".
	Mode string
	// Epsilon is the accuracy tolerance of the Stage 2 classifier used.
	Epsilon int
	// Decision is "stop", "no_stop" or "none".
	Decision string
	// Tau is the number of 100 ms rows seen when the classifier said stop.
	Tau int `json:",omitempty"`
	// PredictedMbps is the predicted full-test throughput (Mbit/s).
	PredictedMbps float64 `json:",omitempty"`
	// Probability is the classifier output at Tau.
	Probability float64 `json:",omitempty"`
	// StopAfterMs is the end of the Tau-th row since the flow started.
	StopAfterMs float64 `json:",omitempty"`
	// FlagSeenMs is when the server saw the stop flag, since the download started.
	FlagSeenMs float64 `json:",omitempty"`
	// FlagLatencyMs is the delay from the decision to the server seeing it.
	FlagLatencyMs float64 `json:",omitempty"`
	// TerminatedEarly is true if the server ended the download because of TurboTest.
	TerminatedEarly bool
}
