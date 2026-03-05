package synchro

import (
	"context"
	"database/sql"
)

// DB is a minimal database interface using database/sql stdlib types only.
// Both *sql.DB and *sql.Tx satisfy this interface, as do sqlx equivalents.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// TxBeginner can start a transaction. *sql.DB satisfies this.
type TxBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// quoteIdentifier wraps a SQL identifier in double quotes with embedded
// quote escaping, preventing SQL injection via column/table names.
func quoteIdentifier(name string) string {
	return `"` + escapeDoubleQuotes(name) + `"`
}

func escapeDoubleQuotes(s string) string {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			n++
		}
	}
	if n == 0 {
		return s
	}
	buf := make([]byte, 0, len(s)+n)
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			buf = append(buf, '"', '"')
		} else {
			buf = append(buf, s[i])
		}
	}
	return string(buf)
}

// expandSlicePlaceholder generates "($N, $N+1, ...)" for a string slice,
// appending each element to args. Returns the SQL fragment and updated args.
// This avoids the need for pq.Array() or any driver-specific array handling.
//
// startIdx is the 1-based parameter index to start from.
// Example: expandSlicePlaceholder([]string{"a","b"}, 3, args) → "($3, $4)", append(args, "a", "b")
func expandSlicePlaceholder(slice []string, startIdx int, args []any) (string, []any, int) {
	if len(slice) == 0 {
		return "(NULL)", args, startIdx
	}
	parts := make([]byte, 0, len(slice)*4)
	parts = append(parts, '(')
	for i, v := range slice {
		if i > 0 {
			parts = append(parts, ',', ' ')
		}
		parts = append(parts, '$')
		parts = appendInt(parts, startIdx)
		args = append(args, v)
		startIdx++
	}
	parts = append(parts, ')')
	return string(parts), args, startIdx
}

func appendInt(buf []byte, n int) []byte {
	if n < 10 {
		return append(buf, byte('0'+n))
	}
	// Simple int-to-ascii for small numbers (parameter indices)
	var tmp [20]byte
	i := len(tmp)
	for n > 0 {
		i--
		tmp[i] = byte('0' + n%10)
		n /= 10
	}
	return append(buf, tmp[i:]...)
}
