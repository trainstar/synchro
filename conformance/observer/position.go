package observer

import (
	"strconv"
	"strings"
)

// ParsePostgreSQLLSN preserves PostgreSQL's two unsigned 32-bit position words.
func ParsePostgreSQLLSN(value string) (uint64, bool) {
	highText, lowText, found := strings.Cut(value, "/")
	if !found || highText == "" || lowText == "" {
		return 0, false
	}
	high, err := strconv.ParseUint(highText, 16, 32)
	if err != nil {
		return 0, false
	}
	low, err := strconv.ParseUint(lowText, 16, 32)
	if err != nil {
		return 0, false
	}
	return high<<32 | low, true
}
