package chstore

import (
	"strings"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	sql := "-- header\nCREATE TABLE a (x UInt8)\nENGINE = Memory;\n\n-- c\nCREATE VIEW v AS SELECT 1;\nSELECT 2"
	got := SplitStatements(sql)
	if len(got) != 3 || !strings.HasPrefix(got[0], "CREATE TABLE a") || strings.HasSuffix(got[0], ";") || got[2] != "SELECT 2" {
		t.Fatalf("got %q", got)
	}
}

func TestEmbeddedMigrationsParse(t *testing.T) {
	for _, f := range []string{"migrations/0001_events_raw.sql", "migrations/0002_aggregates.sql"} {
		b, err := migrationsFS.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if n := len(SplitStatements(string(b))); n == 0 {
			t.Fatalf("%s: no statements", f)
		}
	}
}
