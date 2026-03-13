package server

// QueryMetrics holds per-query resource usage reported by the worker.
type QueryMetrics struct {
	MemoryBytes  int64 // Peak memory used by the query (bytes)
	CPUTimeUs    int64 // CPU time consumed (microseconds)
	BytesScanned int64 // Bytes read from storage
	BytesWritten int64 // Bytes written to storage
}

// MetricsProvider is an optional interface that QueryExecutor implementations
// can implement to provide per-query resource metrics.
type MetricsProvider interface {
	LastQueryMetrics() *QueryMetrics
}
