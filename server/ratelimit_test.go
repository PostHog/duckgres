package server

import (
	"net"
	"testing"
	"time"
)

// mockAddr implements net.Addr for testing
type mockAddr struct {
	network string
	addr    string
}

func (m mockAddr) Network() string { return m.network }
func (m mockAddr) String() string  { return m.addr }

func TestExtractIP(t *testing.T) {
	tests := []struct {
		name     string
		addr     net.Addr
		expected string
	}{
		{
			name:     "IPv4 with port",
			addr:     mockAddr{"tcp", "192.168.1.1:5432"},
			expected: "192.168.1.1",
		},
		{
			name:     "IPv6 with port",
			addr:     mockAddr{"tcp", "[::1]:5432"},
			expected: "::1",
		},
		{
			name:     "localhost with port",
			addr:     mockAddr{"tcp", "127.0.0.1:12345"},
			expected: "127.0.0.1",
		},
		{
			name:     "no port (returns as-is)",
			addr:     mockAddr{"tcp", "192.168.1.1"},
			expected: "192.168.1.1",
		},
		{
			name:     "nil addr",
			addr:     nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractIP(tt.addr)
			if result != tt.expected {
				t.Errorf("extractIP(%v) = %q, want %q", tt.addr, result, tt.expected)
			}
		})
	}
}

func TestRateLimiter_BasicOperations(t *testing.T) {
	cfg := RateLimitConfig{
		MaxFailedAttempts:   3,
		FailedAttemptWindow: 1 * time.Minute,
		BanDuration:         5 * time.Minute,
		MaxConnectionsPerIP: 5,
	}
	rl := NewRateLimiter(cfg)

	addr := mockAddr{"tcp", "192.168.1.100:5432"}

	t.Run("initial connection allowed", func(t *testing.T) {
		msg := rl.CheckConnection(addr)
		if msg != "" {
			t.Errorf("CheckConnection() should allow initial connection, got: %s", msg)
		}
	})

	t.Run("register connection", func(t *testing.T) {
		ok := rl.RegisterConnection(addr)
		if !ok {
			t.Error("RegisterConnection() should succeed")
		}
	})

	t.Run("unregister connection", func(t *testing.T) {
		rl.UnregisterConnection(addr)
		// Should not panic, connection count should be 0
	})

	t.Run("unregister non-existent connection", func(t *testing.T) {
		newAddr := mockAddr{"tcp", "10.0.0.1:5432"}
		rl.UnregisterConnection(newAddr) // Should not panic
	})
}

func TestRateLimiter_ConnectionLimit(t *testing.T) {
	cfg := RateLimitConfig{
		MaxFailedAttempts:   5,
		FailedAttemptWindow: 1 * time.Minute,
		BanDuration:         5 * time.Minute,
		MaxConnectionsPerIP: 3,
	}
	rl := NewRateLimiter(cfg)

	addr := mockAddr{"tcp", "192.168.1.100:5432"}

	// Register max connections
	for i := 0; i < 3; i++ {
		ok := rl.RegisterConnection(addr)
		if !ok {
			t.Errorf("RegisterConnection() %d should succeed", i+1)
		}
	}

	t.Run("exceeds connection limit", func(t *testing.T) {
		ok := rl.RegisterConnection(addr)
		if ok {
			t.Error("RegisterConnection() should fail when at limit")
		}

		msg := rl.CheckConnection(addr)
		if msg == "" {
			t.Error("CheckConnection() should return error when at limit")
		}
		if msg != "too many connections from your IP address" {
			t.Errorf("unexpected error message: %s", msg)
		}
	})

	t.Run("allows connection after unregister", func(t *testing.T) {
		rl.UnregisterConnection(addr)
		ok := rl.RegisterConnection(addr)
		if !ok {
			t.Error("RegisterConnection() should succeed after unregister")
		}
	})
}

func TestRateLimiter_FailedAuth(t *testing.T) {
	cfg := RateLimitConfig{
		MaxFailedAttempts:   3,
		FailedAttemptWindow: 1 * time.Minute,
		BanDuration:         100 * time.Millisecond, // Short for testing
		MaxConnectionsPerIP: 100,
	}
	rl := NewRateLimiter(cfg)

	addr := mockAddr{"tcp", "192.168.1.100:5432"}

	t.Run("not banned after fewer than max attempts", func(t *testing.T) {
		// 2 failed attempts (below threshold of 3)
		rl.RecordFailedAuth(addr)
		rl.RecordFailedAuth(addr)

		if rl.IsBanned(addr) {
			t.Error("Should not be banned after 2 failed attempts (max is 3)")
		}
	})

	t.Run("banned after max attempts", func(t *testing.T) {
		// 3rd failed attempt (reaches threshold)
		banned := rl.RecordFailedAuth(addr)
		if !banned {
			t.Error("RecordFailedAuth() should return true when ban threshold reached")
		}

		if !rl.IsBanned(addr) {
			t.Error("IsBanned() should return true after max failed attempts")
		}

		// Check that connection is rejected
		msg := rl.CheckConnection(addr)
		if msg == "" {
			t.Error("CheckConnection() should return error when banned")
		}
	})

	t.Run("ban expires", func(t *testing.T) {
		// Wait for ban to expire
		time.Sleep(150 * time.Millisecond)

		if rl.IsBanned(addr) {
			t.Error("IsBanned() should return false after ban expires")
		}

		msg := rl.CheckConnection(addr)
		if msg != "" {
			t.Errorf("CheckConnection() should allow connection after ban expires, got: %s", msg)
		}
	})
}

func TestRateLimiter_SuccessfulAuth(t *testing.T) {
	cfg := RateLimitConfig{
		MaxFailedAttempts:   3,
		FailedAttemptWindow: 1 * time.Minute,
		BanDuration:         5 * time.Minute,
		MaxConnectionsPerIP: 100,
	}
	rl := NewRateLimiter(cfg)

	addr := mockAddr{"tcp", "192.168.1.100:5432"}

	// Record some failed attempts (but not enough to ban)
	rl.RecordFailedAuth(addr)
	rl.RecordFailedAuth(addr)

	// Successful auth should clear failed attempts
	rl.RecordSuccessfulAuth(addr)

	// Now 3 more failed attempts should be needed to ban
	rl.RecordFailedAuth(addr)
	rl.RecordFailedAuth(addr)
	if rl.IsBanned(addr) {
		t.Error("Should not be banned - successful auth should have reset counter")
	}

	// 3rd attempt after reset should trigger ban
	banned := rl.RecordFailedAuth(addr)
	if !banned {
		t.Error("Should be banned after 3 failed attempts following reset")
	}
}

func TestRateLimiter_DifferentIPs(t *testing.T) {
	cfg := RateLimitConfig{
		MaxFailedAttempts:   2,
		FailedAttemptWindow: 1 * time.Minute,
		BanDuration:         5 * time.Minute,
		MaxConnectionsPerIP: 100,
	}
	rl := NewRateLimiter(cfg)

	addr1 := mockAddr{"tcp", "192.168.1.1:5432"}
	addr2 := mockAddr{"tcp", "192.168.1.2:5432"}

	// Ban addr1
	rl.RecordFailedAuth(addr1)
	rl.RecordFailedAuth(addr1)

	if !rl.IsBanned(addr1) {
		t.Error("addr1 should be banned")
	}

	// addr2 should not be affected
	if rl.IsBanned(addr2) {
		t.Error("addr2 should not be banned")
	}

	msg := rl.CheckConnection(addr2)
	if msg != "" {
		t.Errorf("addr2 should be allowed, got: %s", msg)
	}
}

func TestRateLimiter_NilAddr(t *testing.T) {
	cfg := DefaultRateLimitConfig()
	rl := NewRateLimiter(cfg)

	// All operations with nil addr should succeed/not panic
	t.Run("CheckConnection nil", func(t *testing.T) {
		msg := rl.CheckConnection(nil)
		if msg != "" {
			t.Errorf("CheckConnection(nil) should return empty, got: %s", msg)
		}
	})

	t.Run("RegisterConnection nil", func(t *testing.T) {
		ok := rl.RegisterConnection(nil)
		if !ok {
			t.Error("RegisterConnection(nil) should return true")
		}
	})

	t.Run("UnregisterConnection nil", func(t *testing.T) {
		rl.UnregisterConnection(nil) // Should not panic
	})

	t.Run("RecordFailedAuth nil", func(t *testing.T) {
		banned := rl.RecordFailedAuth(nil)
		if banned {
			t.Error("RecordFailedAuth(nil) should return false")
		}
	})

	t.Run("RecordSuccessfulAuth nil", func(t *testing.T) {
		rl.RecordSuccessfulAuth(nil) // Should not panic
	})

	t.Run("IsBanned nil", func(t *testing.T) {
		if rl.IsBanned(nil) {
			t.Error("IsBanned(nil) should return false")
		}
	})
}

func TestRateLimiter_UnlimitedConnections(t *testing.T) {
	cfg := RateLimitConfig{
		MaxFailedAttempts:   5,
		FailedAttemptWindow: 1 * time.Minute,
		BanDuration:         5 * time.Minute,
		MaxConnectionsPerIP: 0, // Unlimited
	}
	rl := NewRateLimiter(cfg)

	addr := mockAddr{"tcp", "192.168.1.100:5432"}

	// Should be able to register many connections
	for i := 0; i < 1000; i++ {
		ok := rl.RegisterConnection(addr)
		if !ok {
			t.Errorf("RegisterConnection() %d should succeed with unlimited config", i+1)
			break
		}
	}
}

func TestDefaultRateLimitConfig(t *testing.T) {
	cfg := DefaultRateLimitConfig()

	if cfg.MaxFailedAttempts != 5 {
		t.Errorf("MaxFailedAttempts = %d, want 5", cfg.MaxFailedAttempts)
	}
	if cfg.FailedAttemptWindow != 5*time.Minute {
		t.Errorf("FailedAttemptWindow = %v, want 5m", cfg.FailedAttemptWindow)
	}
	if cfg.BanDuration != 15*time.Minute {
		t.Errorf("BanDuration = %v, want 15m", cfg.BanDuration)
	}
	if cfg.MaxConnectionsPerIP != 100 {
		t.Errorf("MaxConnectionsPerIP = %d, want 100", cfg.MaxConnectionsPerIP)
	}
}
