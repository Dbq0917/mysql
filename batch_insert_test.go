package mysql

import (
	"fmt"
	"strings"
	"testing"
)

// TestBatchInsert10000 exercises the nightly bulk-load shape: a single
// multi-row INSERT carrying 10,000 rows followed by another statement on the
// same pooled connection. The statement and its result set must stay paired,
// otherwise the connection reports "commands out of sync" and every later
// statement on it fails until the connection is rebuilt.
func TestBatchInsert10000(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("DROP TABLE IF EXISTS batch_insert_test")
		dbt.mustExec("CREATE TABLE batch_insert_test (id INT PRIMARY KEY AUTO_INCREMENT, v INT NOT NULL, s VARCHAR(64) NOT NULL)")

		const rows = 10000

		t.Run("multiRowInsert", func(t *testing.T) {
			dbt2 := &DBTest{TB: t, db: dbt.db}
			dbt2.db.SetMaxOpenConns(1)
			dbt2.db.SetMaxIdleConns(1)
			dbt2.mustExec("TRUNCATE TABLE batch_insert_test")

			var query strings.Builder
			query.WriteString("INSERT INTO batch_insert_test (v, s) VALUES ")
			args := make([]any, 0, rows*2)
			for i := range rows {
				if i > 0 {
					query.WriteByte(',')
				}
				query.WriteString("(?, ?)")
				args = append(args, i, fmt.Sprintf("row-%d", i))
			}

			dbt2.mustExec(query.String(), args...)
			assertBatchInsertCount(t, dbt2, rows)
		})

		t.Run("preparedInsert", func(t *testing.T) {
			dbt2 := &DBTest{TB: t, db: dbt.db}
			dbt2.db.SetMaxOpenConns(1)
			dbt2.db.SetMaxIdleConns(1)
			dbt2.mustExec("TRUNCATE TABLE batch_insert_test")

			// One round trip per row is enough to exercise statement/result
			// pairing on the prepared path; the 10k-row bulk case above covers
			// the nightly job's volume.
			const preparedRows = 1000
			stmt, err := dbt2.db.Prepare("INSERT INTO batch_insert_test (v, s) VALUES (?, ?)")
			if err != nil {
				t.Fatal(err)
			}
			for i := range preparedRows {
				if _, err := stmt.Exec(i, fmt.Sprintf("prepared-%d", i)); err != nil {
					stmt.Close()
					t.Fatalf("insert row %d: %v", i, err)
				}
			}
			stmt.Close()
			assertBatchInsertCount(t, dbt2, preparedRows)
		})
	})
}

func assertBatchInsertCount(t *testing.T, dbt *DBTest, want int) {
	t.Helper()
	var got int
	if err := dbt.db.QueryRow("SELECT COUNT(*) FROM batch_insert_test").Scan(&got); err != nil {
		t.Fatalf("statement after bulk insert failed: %v", err)
	}
	if got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
}
