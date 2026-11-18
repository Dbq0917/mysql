// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import "context"

// ReuseReason describes why a connection reset was attempted when a
// connection is checked out of the database/sql connection pool.
type ReuseReason string

const (
	// ReuseClean means the pooled connection carried no session state that
	// needed cleanup and was handed over untouched.
	ReuseClean ReuseReason = "clean"
	// ReuseRolledBackTransaction means the previous user of the connection
	// left an open transaction behind. The driver issued ROLLBACK before the
	// connection was reused so the new request cannot inherit the old
	// transaction snapshot.
	ReuseRolledBackTransaction ReuseReason = "rolled-back-transaction"
	// ReuseRollbackFailed means a leftover transaction could not be rolled
	// back (for example because the server connection was broken). The
	// connection is rejected and database/sql discards it instead of
	// handing it to the new request.
	ReuseRollbackFailed ReuseReason = "rollback-failed"
)

// ReuseEvent is reported through Config.OnReuse every time a pooled
// connection is checked out and ResetSession runs. It makes the cleanliness
// of a reused connection observable and lets operators correlate an
// incident with the exact server connection (ConnectionID) involved.
type ReuseEvent struct {
	// ConnectionID is the server-assigned connection id received in the
	// handshake packet (the same value SHOW PROCESSLIST / CONNECTION_ID()
	// reports). It stays constant for the lifetime of the connection.
	ConnectionID uint32
	// Reason classifies the reset outcome.
	Reason ReuseReason
	// Err is set when Reason is ReuseRollbackFailed and nil otherwise.
	Err error
}

// OnReuseFunc is the signature of Config.OnReuse. Implementations must be
// safe for concurrent use because a connection pool reuses connections from
// multiple goroutines, and must not block on pool operations.
type OnReuseFunc func(context.Context, ReuseEvent)
