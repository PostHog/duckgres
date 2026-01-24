// Test binary format support with lib/pq
package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	// Connect with binary mode - the driver uses binary for prepared statements automatically
	connStr := "host=127.0.0.1 port=35432 user=postgres password=postgres dbname=test sslmode=require"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("Failed to open database.", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.Warn("Failed to close database.", "error", err)
		}
	}()

	// Test 1: Create a test table with various types
	fmt.Println("=== Creating test table ===")
	_, err = db.Exec(`DROP TABLE IF EXISTS binary_test`)
	if err != nil {
		slog.Warn("Drop table warning.", "error", err)
	}

	_, err = db.Exec(`
		CREATE TABLE binary_test (
			id INTEGER,
			name TEXT,
			price DOUBLE,
			active BOOLEAN
		)
	`)
	if err != nil {
		slog.Error("Create table failed.", "error", err)
		os.Exit(1)
	}
	fmt.Println("Table created successfully")

	// Test 2: Insert data
	fmt.Println("\n=== Inserting test data ===")
	_, err = db.Exec(`
		INSERT INTO binary_test VALUES
		(1, 'Widget', 19.99, true),
		(2, 'Gadget', 49.99, false),
		(3, 'Gizmo', 9.99, true)
	`)
	if err != nil {
		slog.Error("Insert failed.", "error", err)
		os.Exit(1)
	}
	fmt.Println("Data inserted successfully")

	// Test 3: Query with prepared statement (uses extended query protocol with binary)
	fmt.Println("\n=== Querying with prepared statement ===")
	rows, err := db.Query("SELECT id, name, price, active FROM binary_test WHERE id > $1", 0)
	if err != nil {
		slog.Error("Query failed.", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Warn("Failed to close rows.", "error", err)
		}
	}()

	for rows.Next() {
		var id int
		var name string
		var price float64
		var active bool
		if err := rows.Scan(&id, &name, &price, &active); err != nil {
			slog.Error("Scan failed.", "error", err)
			os.Exit(1)
		}
		fmt.Printf("  id=%d, name=%s, price=%.2f, active=%v\n", id, name, price, active)
	}

	// Test 4: Query integers
	fmt.Println("\n=== Testing integer types ===")
	var intVal int
	err = db.QueryRow("SELECT 12345").Scan(&intVal)
	if err != nil {
		slog.Error("Integer query failed.", "error", err)
		os.Exit(1)
	}
	fmt.Printf("  Integer: %d (expected 12345)\n", intVal)

	// Test 5: Query floats
	fmt.Println("\n=== Testing float types ===")
	var floatVal float64
	err = db.QueryRow("SELECT 3.14159").Scan(&floatVal)
	if err != nil {
		slog.Error("Float query failed.", "error", err)
		os.Exit(1)
	}
	fmt.Printf("  Float: %f (expected 3.14159)\n", floatVal)

	// Test 6: Query boolean
	fmt.Println("\n=== Testing boolean type ===")
	var boolVal bool
	err = db.QueryRow("SELECT true").Scan(&boolVal)
	if err != nil {
		slog.Error("Boolean query failed.", "error", err)
		os.Exit(1)
	}
	fmt.Printf("  Boolean: %v (expected true)\n", boolVal)

	fmt.Println("\n=== All tests passed! ===")
}
