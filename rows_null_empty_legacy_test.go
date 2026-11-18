// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"testing"
)

// TestRowsNextNullVersusEmpty is the legacy Next-path counterpart of the
// RowsColumnScanner distinction test. It asserts the driver.Values handed to
// database/sql keep SQL NULL (nil) distinct from an empty string (a non-nil
// zero-length []byte), with a normal value row unchanged.
func TestRowsNextNullVersusEmpty(t *testing.T) {
	textRowsData := []struct {
		name   string
		packet []byte
		null   bool
	}{
		{name: "NULL", packet: []byte{0xfb}, null: true},
		{name: "empty string", packet: []byte{0}, null: false},
		{name: "value", packet: []byte{3, 'a', 'b', 'c'}, null: false},
	}
	binaryRowsData := []struct {
		name   string
		packet []byte
		null   bool
	}{
		{name: "NULL", packet: []byte{0, 4}, null: true},
		{name: "empty string", packet: []byte{0, 0, 0}, null: false},
		{name: "value", packet: []byte{0, 0, 3, 'a', 'b', 'c'}, null: false},
	}

	for _, binary := range []bool{false, true} {
		data := textRowsData
		if binary {
			data = binaryRowsData
		}
		for _, row := range data {
			rows, _, _ := rowsTestPacket(binary, []mysqlField{{fieldType: fieldTypeString}}, row.packet, false)
			got := make([]driver.Value, 1)
			if err := rows.Next(got); err != nil {
				t.Fatalf("binary=%v %s: Next: %v", binary, row.name, err)
			}
			if row.null {
				if got[0] != nil {
					t.Fatalf("binary=%v NULL decoded as %#v, want nil", binary, got[0])
				}
				continue
			}
			b, ok := got[0].([]byte)
			if !ok {
				t.Fatalf("binary=%v %s decoded as %T, want []byte", binary, row.name, got[0])
			}
			if b == nil {
				t.Fatalf("binary=%v %s decoded as nil []byte, want a non-nil value", binary, row.name)
			}
			switch row.name {
			case "empty string":
				if len(b) != 0 {
					t.Fatalf("empty string decoded as %q, want %q", b, "")
				}
			case "value":
				if string(b) != "abc" {
					t.Fatalf("value decoded as %q, want abc", b)
				}
			}
		}
	}
}
