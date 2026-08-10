package main

import "testing"

func TestEnvPositiveInt64(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  int64
	}{
		{name: "unset", want: 32},
		{name: "valid", value: "64", want: 64},
		{name: "zero", value: "0", want: 32},
		{name: "negative", value: "-1", want: 32},
		{name: "invalid", value: "many", want: 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_POSITIVE_INT64", tt.value)
			if got := envPositiveInt64("TEST_POSITIVE_INT64", 32); got != tt.want {
				t.Fatalf("envPositiveInt64 = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDefaultPeerFetchMaxBytesTracksBlockSize(t *testing.T) {
	if got, want := defaultPeerFetchMaxBytes(32, 1<<20), int64(32<<20); got != want {
		t.Fatalf("1 MiB blocks: default bytes = %d, want %d", got, want)
	}
	if got, want := defaultPeerFetchMaxBytes(32, 8<<20), int64(256<<20); got != want {
		t.Fatalf("8 MiB blocks: default bytes = %d, want %d", got, want)
	}
	if got, want := defaultPeerFetchMaxBytes(64, 1<<20), int64(64<<20); got != want {
		t.Fatalf("64-way sweep: default bytes = %d, want %d", got, want)
	}
}
