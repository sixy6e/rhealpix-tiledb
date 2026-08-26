package stac

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

// ParseUUIDToUint64Pair converts a STAC Item ID into a pair of uint64 integers (high, low).
// Accepts standard 36-char UUID strings (e.g. "S2B_MSIL1C_20250101T000000_..."),
// hex strings, or generates a deterministic MD5 UUID if the ID is arbitrary text.
func ParseUUIDToUint64Pair(stacID string) (uint64, uint64, error) {
	if stacID == "" {
		return 0, 0, fmt.Errorf("empty STAC ID")
	}

	// try parsing directly as standard UUID format
	u, err := uuid.Parse(stacID)
	if err == nil {
		high := binary.BigEndian.Uint64(u[0:8])
		low := binary.BigEndian.Uint64(u[8:16])
		return high, low, nil
	}

	// fallback: if STAC ID is non-UUID text (e.g., Landsat/Sentinel granule name),
	// generate a deterministic 128-bit MD5 hash pair.
	hash := md5.Sum([]byte(stacID))
	high := binary.BigEndian.Uint64(hash[0:8])
	low := binary.BigEndian.Uint64(hash[8:16])

	return high, low, nil
}

// Uint64PairToUUID converts a high and low uint64 pair back into a standard UUID string.
func Uint64PairToUUID(high, low uint64) string {
	var bytes [16]byte
	binary.BigEndian.PutUint64(bytes[0:8], high)
	binary.BigEndian.PutUint64(bytes[8:16], low)

	u, err := uuid.FromBytes(bytes[:])
	if err != nil {
		return fmt.Sprintf("%016x%016x", high, low)
	}
	return u.String()
}
