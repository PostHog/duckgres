package configstore

import "testing"

func TestByteSecondsToGiBSeconds(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		// 1 GiB-second exactly.
		{"1073741824", "1"},
		// 10 GiB × 1800s.
		{"19327352832000", "18000"},
		// Half a GiB-second — finite decimal (power-of-two denominator).
		{"536870912", "0.5"},
		// One byte-second: 1/2^30 needs the full 30 fractional digits.
		{"1", "0.000000000931322574615478515625"},
		// Zero.
		{"0", "0"},
		// ~100 TiB warehouse × 1 day — exceeds float64's exact-integer range
		// and brushes int64's edge; must stay exact through big-int math.
		{"9503903632588800000", "8851200000"},
	}
	for _, tt := range tests {
		got, err := byteSecondsToGiBSeconds(tt.in)
		if err != nil {
			t.Fatalf("byteSecondsToGiBSeconds(%s): %v", tt.in, err)
		}
		if string(got) != tt.want {
			t.Fatalf("byteSecondsToGiBSeconds(%s) = %s, want %s", tt.in, got, tt.want)
		}
	}
	if _, err := byteSecondsToGiBSeconds("not-a-number"); err == nil {
		t.Fatal("expected error for non-integer input")
	}
}
