package mysql

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func framePacket(seq byte, body []byte) []byte {
	return append([]byte{byte(len(body)), byte(len(body) >> 8), byte(len(body) >> 16), seq}, body...)
}

func desyncRows(t *testing.T, binary bool) (driver.Rows, *mockConn, *mysqlConn) {
	t.Helper()
	conn, mc := newRWMockConn(0)
	columns := []mysqlField{{fieldType: fieldTypeString}}
	// Two packets (header + body each) are queued; any further read, such as an
	// attempt to drain the result set on a poisoned connection, fails fast
	// instead of blocking.
	conn.maxReads = 4
	validRow := []byte{1, 'a'}
	malformedRow := []byte{0xfc, 251, 0}
	if binary {
		// Binary rows start with an 0x00 header byte followed by the null
		// bitmap ((columnCount + 7 + 2) / 8 bytes for one column).
		validRow = append([]byte{0, 0}, validRow...)
		malformedRow = append([]byte{0, 0}, malformedRow...)
	}
	// A valid row followed by a malformed row whose 0xfc length prefix promises
	// 251 bytes but only two payload bytes follow.
	conn.data = append(framePacket(0, validRow), framePacket(1, malformedRow)...)
	base := mysqlRows{mc: mc, rs: resultSet{columns: columns}}
	if binary {
		return &binaryRows{base}, conn, mc
	}
	return &textRows{base}, conn, mc
}

// A decode error in the middle of a result set leaves the protocol position
// unknown. The connection must be invalidated so database/sql discards it
// instead of pairing the next statement with the leftovers of the broken
// stream ("commands out of sync").
func TestRowsInvalidateConnOnDecodeError(t *testing.T) {
	for _, binary := range []bool{false, true} {
		name := "text"
		if binary {
			name = "binary"
		}
		t.Run(name, func(t *testing.T) {
			rows, _, mc := desyncRows(t, binary)
			dest := make([]driver.Value, 1)
			if err := rows.Next(dest); err != nil {
				t.Fatalf("first row: %v", err)
			}
			if err := rows.Next(dest); !errors.Is(err, ErrMalformPkt) {
				t.Fatalf("second row = %v, want ErrMalformPkt", err)
			}
			if !mc.closed.Load() {
				t.Errorf("connection still open after a mid-resultset decode error")
			}
			if mc.IsValid() {
				t.Errorf("IsValid() = true, want false")
			}
			// Closing the rows after the connection is gone must be a safe
			// no-op instead of attempting more reads on the poisoned stream.
			if err := rows.Close(); err != nil {
				t.Errorf("rows.Close() = %v, want nil once the conn is dropped", err)
			}
		})
	}
}

// A transport failure while reading a row already closes the connection inside
// readPacket(); lock that behavior in so the pool never reuses a half-read
// result set.
func TestRowsInvalidateConnOnReadError(t *testing.T) {
	conn, mc := newRWMockConn(0)
	conn.data = framePacket(0, []byte{1, 'a'})
	conn.maxReads = 2 // header and body of the first row succeed, next read fails
	rows := &textRows{mysqlRows{mc: mc, rs: resultSet{columns: []mysqlField{{fieldType: fieldTypeString}}}}}

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("first row: %v", err)
	}
	if err := rows.Next(dest); err == nil {
		t.Fatal("second row = nil, want a transport error")
	}
	if !mc.closed.Load() || mc.IsValid() {
		t.Fatalf("connection reusable after transport error: closed=%v valid=%v", mc.closed.Load(), mc.IsValid())
	}
}

// Reaching the clean EOF of a result set must keep the connection usable.
func TestRowsEOFKeepsConnUsable(t *testing.T) {
	conn, mc := newRWMockConn(0)
	conn.data = append(
		framePacket(0, []byte{1, 'a'}),
		framePacket(1, []byte{iEOF, 0, 0, 0, 0})...,
	)
	rows := &textRows{mysqlRows{mc: mc, rs: resultSet{columns: []mysqlField{{fieldType: fieldTypeString}}}}}

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("first row: %v", err)
	}
	if err := rows.Next(dest); !errors.Is(err, io.EOF) {
		t.Fatalf("second row = %v, want io.EOF", err)
	}
	if mc.closed.Load() {
		t.Error("connection closed after a clean EOF")
	}
	if !mc.IsValid() {
		t.Error("IsValid() = false after a clean EOF")
	}
}
