package iceberg

import (
	"strings"
	"testing"
)

// TestDispatcher_BackendSelection asserts that the SQL produced for a given
// Backend value targets the expected catalog backend. The full dispatcher
// lives in server.AttachIcebergCatalog, but the SQL emitted is the
// observable contract: if Build*Stmt emit the right SQL for the right
// backend, the dispatcher's branching is correct by construction.
//
// This catches "stale field" mistakes — e.g. a row with Backend="lakekeeper"
// but TableBucket still populated must produce Lakekeeper SQL, not
// S3-Tables SQL.
func TestDispatcher_BackendSelection(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantSQL string // a substring uniquely identifying the chosen branch
	}{
		{
			name: "explicit s3_tables",
			cfg: Config{
				Backend:     BackendS3Tables,
				TableBucket: "arn:aws:s3tables:us-east-1:1:bucket/x",
			},
			wantSQL: "ENDPOINT_TYPE 's3_tables'",
		},
		{
			name: "explicit lakekeeper",
			cfg: Config{
				Backend:             BackendLakekeeper,
				LakekeeperEndpoint:  "http://lk/catalog",
				LakekeeperWarehouse: "org-x",
			},
			wantSQL: "AUTHORIZATION_TYPE 'none'",
		},
		{
			name: "empty Backend defaults to lakekeeper even with stale TableBucket",
			cfg: Config{
				Backend:             "",
				TableBucket:         "arn:aws:s3tables:us-east-1:1:bucket/stale",
				LakekeeperEndpoint:  "http://lk/catalog",
				LakekeeperWarehouse: "org-x",
			},
			wantSQL: "AUTHORIZATION_TYPE 'none'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var attach string
			switch tc.cfg.ResolvedBackend() {
			case BackendLakekeeper:
				attach = BuildLakekeeperAttachStmt(tc.cfg)
			case BackendS3Tables:
				attach = BuildIcebergAttachStmt(tc.cfg)
			default:
				t.Fatalf("unhandled backend %q", tc.cfg.ResolvedBackend())
			}
			if !strings.Contains(attach, tc.wantSQL) {
				t.Errorf("ATTACH SQL did not contain expected fragment:\n got:  %s\nwant ⊃: %s", attach, tc.wantSQL)
			}
			// And the wrong-branch fragment must NOT appear.
			var unwanted string
			if tc.cfg.ResolvedBackend() == BackendLakekeeper {
				unwanted = "ENDPOINT_TYPE 's3_tables'"
			} else {
				unwanted = "ACCESS_DELEGATION_MODE"
			}
			if strings.Contains(attach, unwanted) {
				t.Errorf("ATTACH SQL contains wrong-branch fragment %q: %s", unwanted, attach)
			}
		})
	}
}
