package dataset

import (
	"fmt"
	"strconv"
	"strings"
)

// Random is a small fixed algorithm (SplitMix64). Its output does not depend
// on the Go standard library random implementation, so a seed replays the
// same dataset on every platform and Go version.
type Random struct {
	state uint64
}

// NewRandom returns a generator for one seed.
func NewRandom(seed uint64) *Random {
	return &Random{state: seed}
}

// Next returns the next 64-bit value.
func (r *Random) Next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// IntN returns a value in [0, limit).
func (r *Random) IntN(limit int) int {
	return int(r.Next() % uint64(limit))
}

// Skewed returns a value in [0, limit) that prefers small values. About half
// of the draws fall in the lowest quarter.
func (r *Random) Skewed(limit int) int {
	unit := float64(r.Next()>>11) / float64(1<<53)
	return min(int(float64(limit)*unit*unit), limit-1)
}

// UUID returns a canonical version 4 UUID.
func (r *Random) UUID() string {
	high, low := r.Next(), r.Next()
	high = high&^0xf000 | 0x4000
	low = low&^(0xc<<60) | 0x8<<60
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", high>>32, (high>>16)&0xffff, high&0xffff, low>>48, low&0xffffffffffff)
}

// Phrase returns minWords to maxWords words.
func (r *Random) Phrase(minWords, maxWords int) string {
	count := minWords + r.IntN(maxWords-minWords+1)
	parts := make([]string, count)
	for index := range parts {
		parts[index] = words[r.IntN(len(words))]
	}
	return strings.Join(parts, " ")
}

// Paragraph returns text of about minBytes to maxBytes octets. Most values are
// short, and a few approach maxBytes.
func (r *Random) Paragraph(minBytes, maxBytes int) string {
	unit := float64(r.Next()>>11) / float64(1<<53)
	target := minBytes + int(float64(maxBytes-minBytes)*unit*unit*unit)
	var builder strings.Builder
	for builder.Len() < target {
		if builder.Len() != 0 {
			builder.WriteByte(' ')
		}
		builder.WriteString(words[r.IntN(len(words))])
	}
	return builder.String()
}

// Muscles returns a PostgreSQL text array literal of one to three muscles.
func (r *Random) Muscles() string {
	count := 1 + r.IntN(3)
	chosen := make([]string, 0, count)
	for len(chosen) < count {
		value := muscles[r.IntN(len(muscles))]
		if !strings.Contains(strings.Join(chosen, ","), value) {
			chosen = append(chosen, value)
		}
	}
	return muscleArray(chosen)
}

// Date returns a date in 2026.
func (r *Random) Date() string {
	return fmt.Sprintf("2026-%02d-%02d", 1+r.IntN(12), 1+r.IntN(28))
}

// OptionalTimestamp returns NULL or a UTC timestamp with microseconds.
func (r *Random) OptionalTimestamp() any {
	if r.IntN(4) == 0 {
		return nil
	}
	return fmt.Sprintf("2026-%02d-%02dT%02d:%02d:%02d.%06dZ",
		1+r.IntN(12), 1+r.IntN(28), r.IntN(24), r.IntN(60), r.IntN(60), r.IntN(1000000))
}

// OptionalInt returns NULL or a value in [0, limit).
func (r *Random) OptionalInt(limit int) any {
	if r.IntN(5) == 0 {
		return nil
	}
	return r.IntN(limit)
}

// Int64 returns a full-range int64. One draw in eight is above 2^53, where a
// binary64 conversion loses precision.
func (r *Random) Int64() int64 {
	if r.IntN(8) == 0 {
		return int64(1<<53) + int64(r.IntN(1<<20))*2 + 1
	}
	return int64(r.Next())
}

// OptionalInt64 returns NULL or a full-range int64.
func (r *Random) OptionalInt64() any {
	if r.IntN(3) == 0 {
		return nil
	}
	return r.Int64()
}

// Weight returns a NUMERIC(7,2) value in [0, 500].
func (r *Random) Weight() string {
	cents := r.IntN(50001)
	return strconv.Itoa(cents/100) + "." + fmt.Sprintf("%02d", cents%100)
}

// RPE returns NULL, a half-step effort in [6, 10], or a small finite float.
func (r *Random) RPE() any {
	switch roll := r.IntN(20); {
	case roll == 0:
		return nil
	case roll == 1:
		return 1e-7
	default:
		return 6 + float64(r.IntN(9))/2
	}
}

// Bytes returns size deterministic octets.
func (r *Random) Bytes(size int) []byte {
	body := make([]byte, size)
	for index := 0; index < size; index += 8 {
		value := r.Next()
		for offset := 0; offset < 8 && index+offset < size; offset++ {
			body[index+offset] = byte(value >> (8 * offset))
		}
	}
	return body
}
