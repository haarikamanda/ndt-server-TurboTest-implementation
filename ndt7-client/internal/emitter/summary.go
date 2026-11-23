package emitter

// ValueUnitPair represents a {"Value": ..., "Unit": ...} pair.
type ValueUnitPair struct {
	Value float64
	Unit  string
}

// SubtestSummary contains all the results of a single subtest (download or
// upload). All the values are from the server's perspective, except for the
// download throughput.
type SubtestSummary struct {
	// UUID is the unique identified of this subtest.
	UUID string
	// Throughput is the measured throughput during this subtest.
	Throughput ValueUnitPair
	// Latency is the MinRTT value of the latest measurement, in milliseconds.
	Latency ValueUnitPair
	// Retransmission is BytesRetrans / BytesSent from TCPInfo
	Retransmission ValueUnitPair
	// TurboTest is the server's early-termination decision (download only).
	TurboTest *TurboTestSummary `json:",omitempty"`
}

// TurboTestSummary reports a TurboTest decision.
type TurboTestSummary struct {
	Mode            string
	Epsilon         int
	Decision        string
	TerminatedEarly bool
	// PredictedThroughput is TurboTest's estimate of the full-test throughput.
	PredictedThroughput ValueUnitPair
	// StoppedAfter is the time from the start of the download to the stop flag.
	StoppedAfter ValueUnitPair
	Tau          int
	Probability  float64
}

// Summary is a struct containing the values displayed to the user at
// the end of an ndt7 test.
type Summary struct {
	// ServerFQDN is the FQDN of the server used for this test.
	ServerFQDN string

	// ServerIP is the (v4 or v6) IP address of the server.
	ServerIP string

	// ClientIP is the (v4 or v6) IP address of the client.
	ClientIP string

	// Download is a summary of the download subtest.
	Download *SubtestSummary

	// Upload is a summary of the upload subtest.
	Upload *SubtestSummary
}

// NewSummary returns a new Summary struct for a given FQDN.
func NewSummary(FQDN string) *Summary {
	return &Summary{
		ServerFQDN: FQDN,
	}
}
