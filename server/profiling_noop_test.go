package server

// noopProfiling is embedded in test mock executors to satisfy the
// LastProfilingOutput method of the QueryExecutor interface.
type noopProfiling struct{}

func (noopProfiling) LastProfilingOutput() string { return "" }
