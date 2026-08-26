package stac

import (
	"bytes"
	"encoding/json"
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/paulmach/orb/geojson"
)

// STACItem represents the essential STAC properties required for rHEALPix footprint indexing.
type STACItem struct {
	ID         string           `json:"id"`
	BBox       [4]float64       `json:"bbox"`
	Geometry   geojson.Geometry `json:"geometry"`
	Properties struct {
		Datetime   string  `json:"datetime"`
		CloudCover float32 `json:"eo:cloud_cover"`
	} `json:"properties"`
}

// ReadAndCleanSTACStream fetches a STAC JSON payload from VFS, strips invalid NaN literals,
// and parses the Item structure alongside sanitised raw JSON bytes for catalogue storage.
func ReadAndCleanSTACStream(vfs *tiledb.VFS, uri string) (*STACItem, []byte, error) {
	fh, err := vfs.Open(uri, tiledb.TILEDB_VFS_READ)
	if err != nil {
		return nil, nil, fmt.Errorf("opening VFS path %s: %w", uri, err)
	}
	defer fh.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(fh); err != nil {
		return nil, nil, fmt.Errorf("reading VFS stream %s: %w", uri, err)
	}

	rawBytes := buf.Bytes()
	if len(rawBytes) == 0 {
		return nil, nil, fmt.Errorf("empty STAC stream at %s", uri)
	}

	// clean non-standard JSON literals frequently emitted by Python GDAL/STAC tools
	cleanedJSON := bytes.ReplaceAll(rawBytes, []byte(": NaN"), []byte(": null"))
	cleanedJSON = bytes.ReplaceAll(cleanedJSON, []byte(":NaN"), []byte(": null"))
	cleanedJSON = bytes.ReplaceAll(cleanedJSON, []byte(": -Infinity"), []byte(": null"))
	cleanedJSON = bytes.ReplaceAll(cleanedJSON, []byte(": Infinity"), []byte(": null"))

	var item STACItem
	if err := json.Unmarshal(cleanedJSON, &item); err != nil {
		return nil, nil, fmt.Errorf("unmarshaling STAC JSON for %s: %w", uri, err)
	}

	return &item, cleanedJSON, nil
}
