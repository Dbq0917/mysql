// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// reuseRecorder collects every reuse event reported through Config.OnReuse.
type reuseRecorder struct {
	mu     sync.Mutex
	events []ReuseEvent
}

func (r *reuseRecorder) onReuse(_ context.Context, event ReuseEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *reuseRecorder) snapshot() []ReuseEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ReuseEvent(nil), r.events...)
}

func (r *reuseRecorder) byReason(reason ReuseReason) []ReuseEvent {
	var found []ReuseEvent
	for _, event := range r.snapshot() {
		if event.Reason == reason {
			found = append(found, event)
		}
	}
	return found
}

func openReuseDB(t *testing.T, recorder *reuseRecorder) *sql.DB {
	t.Helper()
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	cfg.OnReuse = recorder.onReuse

	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	sqlDB := sql.OpenDB(connector)
	t.Cleanup(func() { sqlDB.Close() })

	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetConnMaxLifetime(0)
	sqlDB.SetConnMaxIdleTime(0)
	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	return sqlDB
}

func mustExecReuse(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("Exec %q: %v", query, err)
	}
}

// driverConnOn runs fn against the *mysqlConn backing a pooled *sql.Conn.
func driverConnOn(t *testing.T, sqlConn *sql.Conn, fn func(*mysqlConn)) {
	t.Helper()
	err := sqlConn.Raw(func(driverConn any) error {
		mc, ok := driverConn.(*mysqlConn)
		if !ok {
			t.Fatalf("driver connection is %T, want *mysqlConn", driverConn)
		}
		fn(mc)
		return nil
	})
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
}

// TestResetSessionRollsBackLeftoverTransaction reproduces the acceptance
// scenario end to end: a connection returned with an open transaction must be
// reset before reuse so the next request cannot inherit the old transaction's
// snapshot, while a normally committed transaction afterwards stays visible.
func TestResetSessionRollsBackLeftoverTransaction(t *testing.T) {
	recorder := &reuseRecorder{}
	db := openReuseDB(t, recorder)
	ctx := t.Context()

	mustExecReuse(t, db, "DROP TABLE IF EXISTS test_reset_session")
	mustExecReuse(t, db, "CREATE TABLE test_reset_session (id INT PRIMARY KEY, val INT)")
	mustExecReuse(t, db, "INSERT INTO test_reset_session (id, val) VALUES (1, 1)")
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS test_reset_session") })

	// Open a real transaction on the single pooled connection through the
	// driver, then "leak" it by returning the connection without Commit or
	// Rollback. This mirrors a request that finishes while its transaction
	// is still open on the server.
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	driverConnOn(t, sqlConn, func(mc *mysqlConn) {
		if _, err := mc.Begin(); err != nil {
			t.Fatalf("driver Begin: %v", err)
		}
		if mc.status&statusInTrans == 0 {
			t.Fatal("server did not report an open transaction after Begin")
		}
		if _, err := mc.Exec("UPDATE test_reset_session SET val = 2 WHERE id = 1", nil); err != nil {
			t.Fatalf("driver Exec: %v", err)
		}
	})
	if err := sqlConn.Close(); err != nil {
		t.Fatalf("returning connection with open transaction: %v", err)
	}

	// Another request takes over the same connection. ResetSession must have
	// rolled the leftover transaction back: the uncommitted change (val = 2)
	// must not be visible and the row must still read its old value.
	var val int
	if err := db.QueryRowContext(ctx, "SELECT val FROM test_reset_session WHERE id = 1").Scan(&val); err != nil {
		t.Fatalf("read after dirty reuse: %v", err)
	}
	if val != 1 {
		t.Fatalf("inherited stale transaction snapshot: val = %d, want 1", val)
	}

	// The connection must be usable for an ordinary, properly committed
	// transaction and the committed value must be visible afterwards.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE test_reset_session SET val = 3 WHERE id = 1"); err != nil {
		t.Fatalf("tx Exec: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT val FROM test_reset_session WHERE id = 1").Scan(&val); err != nil {
		t.Fatalf("read after commit: %v", err)
	}
	if val != 3 {
		t.Fatalf("committed value not visible: val = %d, want 3", val)
	}

	// The reset must be observable and traceable to the exact connection.
	rolledBack := recorder.byReason(ReuseRolledBackTransaction)
	if len(rolledBack) != 1 {
		t.Fatalf("got %d rolled-back-transaction events %v, want 1", len(rolledBack), recorder.snapshot())
	}
	if rolledBack[0].ConnectionID == 0 {
		t.Fatal("reuse event is missing the server connection id")
	}
	if rolledBack[0].Err != nil {
		t.Fatalf("rolled-back event carries unexpected error: %v", rolledBack[0].Err)
	}
	for _, event := range recorder.byReason(ReuseClean) {
		if event.ConnectionID != rolledBack[0].ConnectionID {
			t.Fatalf("clean reuse on connection %d, want same connection %d",
				event.ConnectionID, rolledBack[0].ConnectionID)
		}
		if event.Err != nil {
			t.Fatalf("clean event carries unexpected error: %v", event.Err)
		}
	}
	if failed := recorder.byReason(ReuseRollbackFailed); len(failed) != 0 {
		t.Fatalf("unexpected rollback-failed events: %v", failed)
	}

	// The driver-visible status must no longer claim an open transaction.
	sqlConn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	driverConnOn(t, sqlConn2, func(mc *mysqlConn) {
		if mc.status&statusInTrans != 0 {
			t.Fatal("connection still marked as being in a transaction after reset")
		}
		if mc.connectionID != rolledBack[0].ConnectionID {
			t.Fatalf("connection id changed from %d to %d; pool did not reuse the connection",
				rolledBack[0].ConnectionID, mc.connectionID)
		}
	})
	_ = sqlConn2.Close()
}

// TestResetSessionCleanReuseReportsClean checks that ordinary reuse of a
// connection without leftover state is reported as clean and remains usable.
func TestResetSessionCleanReuseReportsClean(t *testing.T) {
	recorder := &reuseRecorder{}
	db := openReuseDB(t, recorder)
	ctx := t.Context()

	for i := range 3 {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		if n != 1 {
			t.Fatalf("query %d returned %d, want 1", i, n)
		}
	}

	events := recorder.byReason(ReuseClean)
	if len(events) != 3 {
		t.Fatalf("got %d clean events %v, want 3", len(events), recorder.snapshot())
	}
	for _, event := range events {
		if event.ConnectionID == 0 {
			t.Fatal("clean reuse event is missing the server connection id")
		}
		if event.Err != nil {
			t.Fatalf("clean event carries unexpected error: %v", event.Err)
		}
	}
	if dirty := recorder.byReason(ReuseRolledBackTransaction); len(dirty) != 0 {
		t.Fatalf("unexpected dirty reuse events: %v", dirty)
	}
}

// TestResetSessionRollsBackLeftoverReadOnlyTransaction covers read-only
// transactions, which the server flags with a separate status bit in
// addition to statusInTrans.
func TestResetSessionRollsBackLeftoverReadOnlyTransaction(t *testing.T) {
	recorder := &reuseRecorder{}
	db := openReuseDB(t, recorder)
	ctx := t.Context()

	sqlConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	driverConnOn(t, sqlConn, func(mc *mysqlConn) {
		tx, err := mc.begin(true)
		if err != nil {
			t.Fatalf("driver begin read-only: %v", err)
		}
		if mc.status&statusInTrans == 0 {
			t.Fatal("server did not report an open read-only transaction")
		}
		_ = tx
	})
	if err := sqlConn.Close(); err != nil {
		t.Fatalf("returning connection: %v", err)
	}

	var n int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
		t.Fatalf("query after read-only tx leak: %v", err)
	}
	rolledBack := recorder.byReason(ReuseRolledBackTransaction)
	if len(rolledBack) != 1 {
		t.Fatalf("got %d rolled-back events %v, want 1", len(rolledBack), recorder.snapshot())
	}

	sqlConn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	driverConnOn(t, sqlConn2, func(mc *mysqlConn) {
		if mc.status&(statusInTrans|statusInTransReadonly) != 0 {
			t.Fatalf("transaction status bits still set: %b", mc.status)
		}
	})
	_ = sqlConn2.Close()
}

// TestResetSessionRollbackFailureRejectsConn forces the ROLLBACK of a
// leftover transaction to fail at the transport layer and asserts the
// connection is rejected (and reported) instead of being reused in an
// unknown transaction state.
func TestResetSessionRollbackFailureRejectsConn(t *testing.T) {
	mc, _, peerWriter := newPipeTestConn(t)
	recorder := &reuseRecorder{}
	mc.cfg.OnReuse = recorder.onReuse
	mc.connectionID = 4242

	// Simulate a transaction left open by the previous user of this
	// connection, then make the cleanup ROLLBACK unwriteable.
	mc.status |= statusInTrans
	if err := peerWriter.Close(); err != nil {
		t.Fatalf("close peer writer: %v", err)
	}

	err := mc.ResetSession(t.Context())
	if err != driver.ErrBadConn {
		t.Fatalf("ResetSession = %v, want ErrBadConn", err)
	}
	if !mc.closed.Load() {
		t.Fatal("failed reset did not close the connection")
	}

	failed := recorder.byReason(ReuseRollbackFailed)
	if len(failed) != 1 {
		t.Fatalf("got %d rollback-failed events %v, want 1", len(failed), recorder.snapshot())
	}
	if failed[0].ConnectionID != 4242 {
		t.Fatalf("failure event connection id = %d, want 4242", failed[0].ConnectionID)
	}
	if failed[0].Err == nil || errors.Is(failed[0].Err, driver.ErrBadConn) {
		t.Fatalf("failure event error = %v, want the underlying non-BadConn error", failed[0].Err)
	}
	if clean := recorder.byReason(ReuseClean); len(clean) != 0 {
		t.Fatalf("unexpected clean events after failed reset: %v", clean)
	}
}

// TestResetSessionRollbackFailureOnClosedPipe is the read-failure variant:
// the ROLLBACK packet leaves the client but the server side is gone.
func TestResetSessionRollbackFailureOnClosedPipe(t *testing.T) {
	mc, peerReader, peerWriter := newPipeTestConn(t)
	recorder := &reuseRecorder{}
	mc.cfg.OnReuse = recorder.onReuse
	mc.connectionID = 777
	mc.status |= statusInTrans

	// Writes still succeed into the pipe buffer, but reading the OK packet
	// hits EOF because both peer ends are closed.
	if err := peerReader.Close(); err != nil {
		t.Fatalf("close peer reader: %v", err)
	}
	if err := peerWriter.Close(); err != nil {
		t.Fatalf("close peer writer: %v", err)
	}

	if err := mc.ResetSession(t.Context()); err != driver.ErrBadConn {
		t.Fatalf("ResetSession = %v, want ErrBadConn", err)
	}
	if !mc.closed.Load() {
		t.Fatal("failed reset did not close the connection")
	}
	failed := recorder.byReason(ReuseRollbackFailed)
	if len(failed) != 1 || failed[0].ConnectionID != 777 || failed[0].Err == nil {
		t.Fatalf("unexpected failure events: %v", recorder.snapshot())
	}
}

// TestResetSessionNoTransactionIsClean verifies that a connection without an
// open transaction performs no ROLLBACK and is reported clean. A closed peer
// writer would make any accidental write fail loudly.
func TestResetSessionNoTransactionIsClean(t *testing.T) {
	mc, peerReader, _ := newPipeTestConn(t)
	recorder := &reuseRecorder{}
	mc.cfg.OnReuse = recorder.onReuse
	mc.connectionID = 99

	if err := mc.ResetSession(t.Context()); err != nil {
		t.Fatalf("ResetSession: %v", err)
	}
	if mc.closed.Load() {
		t.Fatal("clean reset closed the connection")
	}
	clean := recorder.byReason(ReuseClean)
	if len(clean) != 1 || clean[0].ConnectionID != 99 || clean[0].Err != nil {
		t.Fatalf("unexpected reuse events: %v", recorder.snapshot())
	}

	// Nothing may have been written to the server on the clean path.
	_ = peerReader.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := peerReader.Read(make([]byte, 1)); err == nil {
		t.Fatal("clean reset wrote data to the server")
	}
}

// BenchmarkResetSessionClean measures the steady-state cost of reusing a
// clean connection: it must stay a couple of status checks plus one optional
// hook nil check, with no extra round trip.
func newPipeBenchConn(b *testing.B) *mysqlConn {
	b.Helper()
	reader, peerWriter, err := os.Pipe()
	if err != nil {
		b.Fatal(err)
	}
	peerReader, writer, err := os.Pipe()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { reader.Close(); peerWriter.Close(); peerReader.Close(); writer.Close() })
	conn := &pipeConn{File: reader, writer: writer}
	deadline := time.Now().Add(time.Hour)
	for _, f := range []*os.File{reader, peerWriter, peerReader, writer} {
		if err := f.SetDeadline(deadline); err != nil {
			b.Fatal(err)
		}
	}
	cfg := NewConfig()
	cfg.Logger = &NopLogger{}
	cfg.ReadTimeout = time.Second
	return &mysqlConn{
		netConn:          conn,
		rawConn:          conn,
		cfg:              cfg,
		buf:              newBuffer(),
		closech:          make(chan struct{}),
		maxAllowedPacket: maxPacketSize,
		connectionID:     1,
	}
}

func BenchmarkResetSessionClean(b *testing.B) {
	mc := newPipeBenchConn(b)
	b.Cleanup(mc.close)
	b.ResetTimer()
	for range b.N {
		if err := mc.ResetSession(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkResetSessionCleanWithHook measures the same path with an OnReuse
// hook installed, showing observability adds only the hook call.
func BenchmarkResetSessionCleanWithHook(b *testing.B) {
	mc := newPipeBenchConn(b)
	b.Cleanup(mc.close)
	mc.cfg.OnReuse = func(context.Context, ReuseEvent) {}
	b.ResetTimer()
	for range b.N {
		if err := mc.ResetSession(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
