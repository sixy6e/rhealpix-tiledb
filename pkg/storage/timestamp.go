package storage

import (
	"fmt"
	"time"
)

// NowMillis returns the current UTC timestamp in milliseconds for ingestion runs.
func NowMillis() uint64 {
	return uint64(time.Now().UnixMilli())
}

// ParseRFC3339ToMillis parses an RFC3339 string (e.g. "2026-03-15T10:00:00Z") into
// milliseconds for tiledb.TILEDB_DATETIME_MS datatype.
func ParseRFC3339ToMillis(ts string) (uint64, error) {
	if ts == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return 0, fmt.Errorf("invalid RFC3339 timestamp '%s': %w", ts, err)
	}
	if t.UnixMilli() < 0 {
		return 0, fmt.Errorf("timestamp '%s' precedes Unix epoch", ts)
	}
	return uint64(t.UnixMilli()), nil
}

// FormatMillisToRFC3339 converts a TileDB millisecond timestamp to standard UTC RFC3339 string.
func FormatMillisToRFC3339(ms uint64) string {
	if ms == 0 {
		return "Beginning of Time"
	}
	return time.UnixMilli(int64(ms)).UTC().Format(time.RFC3339)
}
