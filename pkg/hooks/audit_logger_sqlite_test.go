package hooks

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/square/p2/pkg/logging"
)

func initSQLiteAuditLogger(t *testing.T) (AuditLogger, string, *sql.DB) {
	tempDir, err := ioutil.TempDir("", "hooks_audit_log")
	if err != nil {
		t.Fatalf("Could not set up for hook audit logger test.")
	}

	dbPath := filepath.Join(tempDir, "hooks.db")
	logger := logging.TestLogger()
	auditLogger, err := NewSQLiteAuditLogger(dbPath, &logger)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	return auditLogger, db
}

func TestSQLiteAuditLogger(t *testing.T) {
	al, tempDir, db := initSQLiteAuditLogger(t)
	defer os.RemoveAll(tempDir)
	al.LogFailure(&HookExecContext{
		Name: "sky",
		env: HookExecutionEnvironment{
			HookedPodIDEnvVar:        "pod",
			HookedPodUniqueKeyEnvVar: "deadbeef",
			HookEventEnvVar:          "before_install"},
	}, nil)

	rows, err := db.Query("SELECT COUNT(*) FROM hook_results")
	if err != nil {
		t.Fatalf("unable to query sqlite database: %v", err)
	}
	var count int
	rows.Scan(&count)
	if count < 1 {
		t.Fatal("Found no hook results in the DB")
	}
}
