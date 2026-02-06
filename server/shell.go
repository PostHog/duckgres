package server

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

// RunShell starts an interactive SQL shell with a fully initialized DuckDB connection.
// It uses the same CreateDBConnection path as the PostgreSQL server, so extensions,
// DuckLake, and pg_catalog views are all available.
func RunShell(cfg Config) {
	sem := make(chan struct{}, 1)

	db, err := CreateDBConnection(cfg, sem, "shell")
	if err != nil {
		slog.Error("Failed to create database connection.", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	stopRefresh := StartCredentialRefresh(db, cfg.DuckLake)
	defer stopRefresh()

	fmt.Fprintln(os.Stderr, "Duckgres shell (type \\q to exit)")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, bufio.MaxScanTokenSize), 1024*1024) // 1MB max line

	var buf strings.Builder
	promptPrimary := "duckgres> "
	promptContinuation := "       -> "

	// mu protects queryCancel and buf from concurrent access
	// between the main goroutine and the signal goroutine.
	var mu sync.Mutex
	var queryCancel context.CancelFunc

	// Channel for query cancellation via Ctrl+C
	cancelCh := make(chan os.Signal, 1)
	signal.Notify(cancelCh, syscall.SIGINT)

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancelCh:
				mu.Lock()
				cancel := queryCancel
				if cancel != nil {
					mu.Unlock()
					cancel()
				} else {
					// Not in a query -- clear the current input buffer.
					buf.Reset()
					mu.Unlock()
					fmt.Fprint(os.Stderr, "\n"+promptPrimary)
				}
			case <-done:
				return
			}
		}
	}()

	defer func() {
		signal.Stop(cancelCh)
		close(done)
	}()

	fmt.Fprint(os.Stderr, promptPrimary)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Handle quit commands on empty buffer
		mu.Lock()
		bufEmpty := buf.Len() == 0
		mu.Unlock()
		if bufEmpty && (trimmed == `\q` || trimmed == ".quit" || trimmed == ".exit") {
			break
		}

		mu.Lock()
		if buf.Len() > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(line)

		// Check if statement is complete (ends with semicolon).
		// NOTE: This is a simple heuristic -- it doesn't handle semicolons inside
		// string literals or comments (e.g. SELECT 'a;b'). Acceptable for a REPL.
		accumulated := strings.TrimSpace(buf.String())
		mu.Unlock()

		if accumulated == "" {
			fmt.Fprint(os.Stderr, promptPrimary)
			continue
		}
		if !strings.HasSuffix(accumulated, ";") {
			fmt.Fprint(os.Stderr, promptContinuation)
			continue
		}

		// Execute the statement
		ctx, cancel := context.WithCancel(context.Background())
		mu.Lock()
		queryCancel = cancel
		mu.Unlock()

		executeShellQuery(ctx, db, accumulated)
		cancel()

		mu.Lock()
		queryCancel = nil
		buf.Reset()
		mu.Unlock()

		fmt.Fprint(os.Stderr, promptPrimary)
	}

	fmt.Fprintln(os.Stderr)
}

// executeShellQuery runs a single SQL statement and prints results to stdout.
func executeShellQuery(ctx context.Context, db *sql.DB, query string) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		return
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		return
	}

	if len(cols) == 0 {
		// DDL/DML with no result columns
		fmt.Println("OK")
		return
	}

	// Collect all rows into string slices
	var allRows [][]string
	scanArgs := make([]any, len(cols))
	scanPtrs := make([]sql.NullString, len(cols))
	for i := range scanPtrs {
		scanArgs[i] = &scanPtrs[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
			return
		}
		row := make([]string, len(cols))
		for i, ns := range scanPtrs {
			if ns.Valid {
				row[i] = ns.String
			} else {
				row[i] = "NULL"
			}
		}
		allRows = append(allRows, row)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		return
	}

	// Compute column widths
	widths := make([]int, len(cols))
	for i, c := range cols {
		widths[i] = len(c)
	}
	for _, row := range allRows {
		for i, v := range row {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}

	// Print header
	var header strings.Builder
	var separator strings.Builder
	for i, c := range cols {
		if i > 0 {
			header.WriteString(" | ")
			separator.WriteString("-+-")
		}
		header.WriteString(fmt.Sprintf("%-*s", widths[i], c))
		separator.WriteString(strings.Repeat("-", widths[i]))
	}
	fmt.Println(header.String())
	fmt.Println(separator.String())

	// Print rows
	for _, row := range allRows {
		var line strings.Builder
		for i, v := range row {
			if i > 0 {
				line.WriteString(" | ")
			}
			line.WriteString(fmt.Sprintf("%-*s", widths[i], v))
		}
		fmt.Println(line.String())
	}

	fmt.Fprintf(os.Stderr, "(%d row%s)\n", len(allRows), plural(len(allRows)))
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
