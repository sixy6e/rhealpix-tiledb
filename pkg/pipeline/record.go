package pipeline

import (
	"time"

	rhealpix "github.com/sixy6e/go-rhealpix"
)

// IngestRecord represents a fully processed scene, decomposed into rHEALPix cells
// and ready for dual-array TileDB insertion.
// TODO; cater generally for all STAC docs
type IngestRecord struct {
	UUIDHigh       uint64
	UUIDLow        uint64
	SceneID        string
	Datetime       time.Time
	CloudCover     float32
	CompactedCells []rhealpix.CellID64 // Or CellID128 depending on resolution needs (TODO)
	STACJSON       []byte
}
