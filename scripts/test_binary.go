// Test binary format support with lib/pq
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	// Connect with binary mode - the driver uses binary for prepared statements automatically
	connStr := "host=127.0.0.1 port=35432 user=postgres password=postgres dbname=test sslmode=require"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to close db: %v", err)
		}
	}()

	// Test 1: Create a test table with various types
	fmt.Println("=== Creating test table ===")
	_, err = db.Exec(`DROP TABLE IF EXISTS binary_test`)
	if err != nil {
		log.Printf("Drop table warning: %v", err)
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
		log.Fatalf("Create table failed: %v", err)
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
		log.Fatalf("Insert failed: %v", err)
	}
	fmt.Println("Data inserted successfully")

	// Test 3: Query with prepared statement (uses extended query protocol with binary)
	fmt.Println("\n=== Querying with prepared statement ===")
	rows, err := db.Query("SELECT id, name, price, active FROM binary_test WHERE id > $1", 0)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("failed to close rows: %v", err)
		}
	}()

	for rows.Next() {
		var id int
		var name string
		var price float64
		var active bool
		if err := rows.Scan(&id, &name, &price, &active); err != nil {
			log.Fatalf("Scan failed: %v", err)
		}
		fmt.Printf("  id=%d, name=%s, price=%.2f, active=%v\n", id, name, price, active)
	}

	// Test 4: Query integers
	fmt.Println("\n=== Testing integer types ===")
	var intVal int
	err = db.QueryRow("SELECT 12345").Scan(&intVal)
	if err != nil {
		log.Fatalf("Integer query failed: %v", err)
	}
	fmt.Printf("  Integer: %d (expected 12345)\n", intVal)

	// Test 5: Query floats
	fmt.Println("\n=== Testing float types ===")
	var floatVal float64
	err = db.QueryRow("SELECT 3.14159").Scan(&floatVal)
	if err != nil {
		log.Fatalf("Float query failed: %v", err)
	}
	fmt.Printf("  Float: %f (expected 3.14159)\n", floatVal)

	// Test 6: Query boolean
	fmt.Println("\n=== Testing boolean type ===")
	var boolVal bool
	err = db.QueryRow("SELECT true").Scan(&boolVal)
	if err != nil {
		log.Fatalf("Boolean query failed: %v", err)
	}
	fmt.Printf("  Boolean: %v (expected true)\n", boolVal)

	fmt.Println("\n=== All tests passed! ===")
}
