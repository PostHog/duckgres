# Bounded stall reproduction

Experiment branch only. Worker memory is 64Gi, DuckDB threads remain 8 (3 CPU). The runner image selects pgwire_uncached only; the checked-in full scenario keeps all five targets. Scenario deadline is 30m, Go test deadline 35m, workflow deadline 60m including setup and cleanup.

DUCKGRES_STALL_DIAGNOSTICS=1 enables phase markers and SIGUSR2 private Go stack files, documented in internal/stalldiagnostics/README.md. No diagnostic stack files are uploaded by the workflow. Collect them locally through authorized Kubernetes access before teardown.

The debugger image bundles GDB and procps. Attach it as an ephemeral container targeting duckdb-worker with only SYS_PTRACE added, then capture `thread apply all bt` with GDB logging to a private file, detach, and copy that file locally. Attach briefly pauses the process. Capture twice alongside CPU/I/O counters; do not infer a deadlock from one sleeping stack. Container and temporary files disappear with the test namespace. Do not publish raw stack contents or dataset metadata.
