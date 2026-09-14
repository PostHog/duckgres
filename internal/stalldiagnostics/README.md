# Temporary stall diagnostics

Disabled by default. Set `DUCKGRES_STALL_DIAGNOSTICS=1` on the diagnostic worker to enable fixed phase begin/end markers for schema discovery, DoGet query execution, and streaming. Markers contain no SQL, session IDs, results, or error text.

Send `SIGUSR2` to the worker process to capture all Go goroutines without stopping it. Each capture writes `/tmp/duckgres-stall-<pid>-<timestamp>.txt` with mode `0600`, refusing to overwrite existing files. Retrieve it through authorized private inspection before teardown. Never upload these files to public workflow artifacts or print their contents in CI logs. Native DuckDB threads require a separate native debugger capture.

Disable the environment variable or remove the temporary deployment after investigation. Captures remain only in the pod filesystem and are removed with the pod. If capture fails, only a failure boolean is logged; check temporary filesystem space and permissions through private inspection. Never signal an uninstrumented process expecting a capture.
