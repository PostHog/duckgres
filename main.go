package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/posthog/duckgres/server"
)

func main() {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll("./data", 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Configure the server
	cfg := server.Config{
		Host:     "0.0.0.0",
		Port:     5432,
		DataDir:  "./data",
		Users: map[string]string{
			"postgres": "postgres", // default user/password
			"alice":    "alice123",
			"bob":      "bob123",
		},
	}

	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		srv.Close()
		os.Exit(0)
	}()

	log.Printf("Starting Duckgres server on %s:%d", cfg.Host, cfg.Port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
