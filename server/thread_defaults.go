package server

const cpuMillicoresPerDefaultDuckDBThread int64 = 400

// DefaultDuckDBThreads returns the default DuckDB thread count for a CPU
// allocation expressed in millicores. DuckDB gets 2.5 threads per CPU, rounded
// up so fractional CPU allocations never lose their share of parallelism.
func DefaultDuckDBThreads(cpuMillicores int64) int {
	if cpuMillicores <= 0 {
		return 0
	}
	threads := cpuMillicores / cpuMillicoresPerDefaultDuckDBThread
	if cpuMillicores%cpuMillicoresPerDefaultDuckDBThread != 0 {
		threads++
	}
	return int(threads)
}
