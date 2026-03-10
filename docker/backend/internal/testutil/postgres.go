package testutil

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

const PostgresTestDatabaseURLEnv = "EVENTS_TEST_DATABASE_URL"

func NewIsolatedPostgresDatabase(t *testing.T) string {
	t.Helper()

	baseURL := strings.TrimSpace(os.Getenv(PostgresTestDatabaseURLEnv))
	if baseURL == "" {
		t.Skipf("%s is not set", PostgresTestDatabaseURLEnv)
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse %s: %v", PostgresTestDatabaseURLEnv, err)
	}

	adminDB, err := sql.Open("postgres", baseURL)
	if err != nil {
		t.Fatalf("open admin database: %v", err)
	}

	dbName := isolatedDatabaseName(t.Name())
	if _, err := adminDB.Exec(`CREATE DATABASE ` + pq.QuoteIdentifier(dbName)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create test database %q: %v", dbName, err)
	}

	t.Cleanup(func() {
		_, _ = adminDB.Exec(`
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, dbName)
		_, _ = adminDB.Exec(`DROP DATABASE IF EXISTS ` + pq.QuoteIdentifier(dbName))
		_ = adminDB.Close()
	})

	parsed.Path = "/" + dbName
	return parsed.String()
}

func isolatedDatabaseName(testName string) string {
	normalized := strings.ToLower(strings.TrimSpace(testName))
	normalized = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' {
			return r
		}
		if r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, normalized)
	normalized = strings.Trim(normalized, "_")
	if normalized == "" {
		normalized = "postgres_test"
	}

	return fmt.Sprintf("%s_%s", normalized, strconv.FormatInt(time.Now().UnixNano(), 36))
}
