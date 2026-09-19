//go:build kubernetes

package controlplane

import "testing"

func TestTrinoHoglakeManagedConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, uri, path, namespace string
		fail                       bool
	}{
		{name: "disabled"},
		{name: "default namespace", uri: "http://hoglake.example:8080", path: "s3://example-bucket/trino/"},
		{name: "explicit namespace", uri: "https://hoglake.example", path: "s3://example-bucket/trino/", namespace: "analytics"},
		{name: "missing path", uri: "http://hoglake.example", fail: true},
		{name: "missing uri", path: "s3://example-bucket/trino/", fail: true},
		{name: "api path rejected", uri: "http://hoglake.example/v1", path: "s3://example-bucket/trino/", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envTrinoManagedHoglakeURI, tc.uri)
			t.Setenv(envTrinoHoglakeDataPath, tc.path)
			t.Setenv(envTrinoHoglakeNamespace, tc.namespace)
			cfg, err := trinoManagedHoglakeConfig()
			if (err != nil) != tc.fail {
				t.Fatalf("config=%v err=%v", cfg, err)
			}
			if tc.name == "disabled" && cfg != nil {
				t.Fatal("unexpected managed backend")
			}
			if tc.name == "default namespace" && cfg.Namespace != "main" {
				t.Fatal("incorrect namespace default")
			}
		})
	}
}
