package configresolve

import (
	"testing"

	"github.com/posthog/duckgres/configloader"
)

func TestResolveEffectiveQueryLogKafkaGroupID(t *testing.T) {
	resolved := ResolveEffective(&configloader.FileConfig{
		QueryLog: configloader.QueryLogFileConfig{
			Kafka: configloader.QueryLogKafkaFileConfig{
				GroupID: "yaml-group",
			},
		},
	}, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_QUERY_LOG_KAFKA_GROUP_ID" {
			return "env-group"
		}
		return ""
	}, nil)

	if got := resolved.Server.QueryLog.Kafka.GroupID; got != "env-group" {
		t.Fatalf("expected env Kafka group id to win, got %q", got)
	}
}
