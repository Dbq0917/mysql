// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build go1.27

package mysql

import (
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"
)

// nullEmptyRow is one row of the report field used to tell "not filled"
// (SQL NULL) apart from "filled with an empty string".
type nullEmptyRow struct {
	name    string
	payload []byte
	isNull  bool
}

func nullEmptyRows() []nullEmptyRow {
	return []nullEmptyRow{
		// Text protocol: NULL is the single 0xfb byte, an empty string is a
		// length-encoded value with a zero length.
		{name: "NULL", payload: []byte{0xfb}, isNull: true},
		{name: "empty string", payload: []byte{0}, isNull: false},
		{name: "value", payload: []byte{3, 'a', 'b', 'c'}, isNull: false},
	}
}

func binaryNullEmptyRows() []nullEmptyRow {
	return []nullEmptyRow{
		// Binary protocol: the NULL bitmap marks column 0 at bit (0+2)=2,
		// so a NULL row carries no value payload at all.
		{name: "NULL", payload: []byte{0, 4}, isNull: true},
		{name: "empty string", payload: []byte{0, 0, 0}, isNull: false},
		{name: "value", payload: []byte{0, 0, 3, 'a', 'b', 'c'}, isNull: false},
	}
}

func nullEmptyScanner(t *testing.T, binary bool, row nullEmptyRow) driver.RowsColumnScanner {
	t.Helper()
	rows, _, _ := scannerTestRows(binary, []mysqlField{{fieldType: fieldTypeString}}, row.payload, false)
	if err := rows.NextRow(); err != nil {
		t.Fatalf("%s: NextRow: %v", row.name, err)
	}
	return rows
}

// TestRowsColumnScannerNullVersusEmpty locks the NULL/empty-string
// distinction for every scan destination a report struct can use. A NULL
// must surface as a nil pointer or the explicit Valid=false marker, while an
// empty string stays a present, non-nil value. The value row keeps the
// historical behavior.
func TestRowsColumnScannerNullVersusEmpty(t *testing.T) {
	for _, binary := range []bool{false, true} {
		protocol := "text"
		rows := nullEmptyRows()
		if binary {
			protocol = "binary"
			rows = binaryNullEmptyRows()
		}
		t.Run(protocol, func(t *testing.T) {
			for _, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					scanner := nullEmptyScanner(t, binary, row)
					ctx := driver.ScanContext{}

					// Pointer destination: NULL leaves the pointer nil; an
					// empty string allocates a pointer to "".
					var sp *string
					if err := scanner.ScanColumn(ctx, 0, &sp); err != nil {
						t.Fatalf("**string: %v", err)
					}
					if row.isNull {
						if sp != nil {
							t.Fatalf("NULL scanned into **string = %q, want nil", *sp)
						}
					} else if sp == nil {
						t.Fatalf("%s scanned into **string is nil, want non-nil", row.name)
					}

					// Explicit marker: Valid records whether the column was set.
					var ns sql.NullString
					if err := scanner.ScanColumn(ctx, 0, &ns); err != nil {
						t.Fatalf("NullString: %v", err)
					}
					if ns.Valid == row.isNull {
						t.Fatalf("NullString.Valid = %v for %s", ns.Valid, row.name)
					}

					// Byte destinations keep nil (NULL) distinct from a
					// non-nil zero-length slice (empty string).
					var b []byte
					if err := scanner.ScanColumn(ctx, 0, &b); err != nil {
						t.Fatalf("*[]byte: %v", err)
					}
					if (b == nil) != row.isNull {
						t.Fatalf("[]byte = %#v for %s", b, row.name)
					}

					var raw sql.RawBytes
					if err := scanner.ScanColumn(ctx, 0, &raw); err != nil {
						t.Fatalf("RawBytes: %v", err)
					}
					if (raw == nil) != row.isNull {
						t.Fatalf("RawBytes = %#v for %s", raw, row.name)
					}

					// Interface destinations receive an untyped nil for NULL
					// and a boxed empty []byte for the empty string.
					var anyValue any
					if err := scanner.ScanColumn(ctx, 0, &anyValue); err != nil {
						t.Fatalf("*any: %v", err)
					}
					if (anyValue == nil) != row.isNull {
						t.Fatalf("any = %#v for %s", anyValue, row.name)
					}

					// A custom Scanner receives the same distinction: nil for
					// NULL, a non-nil empty slice for the empty string.
					capture := new(columnScannerCapture)
					if err := scanner.ScanColumn(ctx, 0, capture); err != nil {
						t.Fatalf("Scanner: %v", err)
					}
					if (capture.value == nil) != row.isNull {
						t.Fatalf("Scanner value = %#v for %s", capture.value, row.name)
					}
					if !row.isNull {
						got, ok := capture.value.([]byte)
						if !ok {
							t.Fatalf("Scanner value type = %T, want []byte", capture.value)
						}
						if len(got) != 0 && row.name == "empty string" {
							t.Fatalf("empty string Scanner value = %q, want empty", got)
						}
						if row.name == "value" && string(got) != "abc" {
							t.Fatalf("value Scanner value = %q, want abc", got)
						}
					}

					// Plain string is not a nullable destination. NULL must be
					// rejected with a clear error instead of silently becoming
					// "", while the empty string scans cleanly.
					var s string
					err := scanner.ScanColumn(ctx, 0, &s)
					if row.isNull {
						if err == nil {
							t.Fatal("NULL scanned into *string without error")
						}
						if !strings.Contains(err.Error(), "NULL") {
							t.Fatalf("NULL into *string error = %q, want it to mention NULL", err)
						}
						if s != "" {
							t.Fatalf("failed NULL scan mutated *string to %q", s)
						}
					} else if err != nil {
						t.Fatalf("%s into *string: %v", row.name, err)
					}
				})
			}
		})
	}
}

// TestRowsColumnScannerNullEmptyTypeErrors checks that type mismatches name
// the offending value explicitly for both NULL and empty inputs, so callers
// can tell which row caused the failure.
func TestRowsColumnScannerNullEmptyTypeErrors(t *testing.T) {
	cases := []struct {
		binary bool
		rows   []nullEmptyRow
	}{
		{binary: false, rows: nullEmptyRows()},
		{binary: true, rows: binaryNullEmptyRows()},
	}
	for _, tc := range cases {
		for _, row := range tc.rows {
			scanner := nullEmptyScanner(t, tc.binary, row)
			var number int64
			err := scanner.ScanColumn(driver.ScanContext{}, 0, &number)
			if err == nil {
				t.Fatalf("%s: scanning a string column into *int64 succeeded", row.name)
			}
			if row.isNull && !strings.Contains(err.Error(), "NULL") {
				t.Fatalf("NULL into *int64 error = %q, want it to mention NULL", err)
			}
			if !row.isNull && !strings.Contains(err.Error(), "invalid syntax") {
				t.Fatalf("%s into *int64 error = %q, want it to report invalid syntax", row.name, err)
			}
		}
	}
}
