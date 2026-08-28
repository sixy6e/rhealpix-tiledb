package storage

import (
	"context"
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/sixy6e/rhealpix-tiledb/pkg/pipeline"
)

// WriteDualArrayBatch flushes a batch of IngestRecords simultaneously into
// both footprint-cells.tiledb and scene-catalogue.tiledb.
func WriteDualArrayBatch(
	ctx context.Context,
	tdbCtx *tiledb.Context,
	cellsURI, catalogueURI string,
	records []*pipeline.IngestRecord,
) error {
	if len(records) == 0 {
		return nil
	}

	// ------------------------------------------------------------------------
	// flatten and write footprint cells index
	// ------------------------------------------------------------------------
	var totalCells int
	for _, rec := range records {
		totalCells += len(rec.CompactedCells)
	}

	cellIDs := make([]uint64, 0, totalCells)
	datetimes := make([]int64, 0, totalCells)
	uuidHighs := make([]uint64, 0, totalCells)
	uuidLows := make([]uint64, 0, totalCells)

	for _, rec := range records {
		dtNS := rec.Datetime.UnixNano()
		for _, cell := range rec.CompactedCells {
			cellIDs = append(cellIDs, cell.Uint64())
			datetimes = append(datetimes, dtNS)
			uuidHighs = append(uuidHighs, rec.UUIDHigh)
			uuidLows = append(uuidLows, rec.UUIDLow)
		}
	}

	cellsArray, err := tiledb.NewArray(tdbCtx, cellsURI)
	if err != nil {
		return fmt.Errorf("opening cells array: %w", err)
	}
	defer cellsArray.Free()

	if err := cellsArray.Open(tiledb.TILEDB_WRITE); err != nil {
		return fmt.Errorf("opening cells array for write: %w", err)
	}
	defer cellsArray.Close()

	queryCells, err := tiledb.NewQuery(tdbCtx, cellsArray)
	if err != nil {
		return fmt.Errorf("creating cells query: %w", err)
	}
	defer queryCells.Free()

	if _, err := queryCells.SetDataBuffer("cell_id", cellIDs); err != nil {
		return err
	}
	if _, err := queryCells.SetDataBuffer("datetime", datetimes); err != nil {
		return err
	}
	if _, err := queryCells.SetDataBuffer("uuid_high", uuidHighs); err != nil {
		return err
	}
	if _, err := queryCells.SetDataBuffer("uuid_low", uuidLows); err != nil {
		return err
	}

	if err := queryCells.Submit(); err != nil {
		return fmt.Errorf("submitting cells write query: %w", err)
	}

	// ------------------------------------------------------------------------
	// flatten and write scene catalogue (STAC metadata)
	// ------------------------------------------------------------------------
	catHighs := make([]uint64, 0, len(records))
	catLows := make([]uint64, 0, len(records))
	stacData := make([]byte, 0, len(records)*2000)
	stacOffsets := make([]uint64, 0, len(records))

	var currentOffset uint64
	for _, rec := range records {
		catHighs = append(catHighs, rec.UUIDHigh)
		catLows = append(catLows, rec.UUIDLow)
		stacOffsets = append(stacOffsets, currentOffset)

		stacData = append(stacData, rec.STACJSON...)
		currentOffset += uint64(len(rec.STACJSON))
	}

	catalogueArray, err := tiledb.NewArray(tdbCtx, catalogueURI)
	if err != nil {
		return fmt.Errorf("opening catalogue array: %w", err)
	}
	defer catalogueArray.Free()

	if err := catalogueArray.Open(tiledb.TILEDB_WRITE); err != nil {
		return fmt.Errorf("opening catalogue array for write: %w", err)
	}
	defer catalogueArray.Close()

	queryCatalogue, err := tiledb.NewQuery(tdbCtx, catalogueArray)
	if err != nil {
		return fmt.Errorf("creating catalogue query: %w", err)
	}
	defer queryCatalogue.Free()

	if _, err := queryCatalogue.SetDataBuffer("uuid_high", catHighs); err != nil {
		return err
	}
	if _, err := queryCatalogue.SetDataBuffer("uuid_low", catLows); err != nil {
		return err
	}
	if _, err := queryCatalogue.SetDataBuffer("stac_json", stacData); err != nil {
		return err
	}
	if _, err := queryCatalogue.SetOffsetsBuffer("stac_json", stacOffsets); err != nil {
		return err
	}

	if err := queryCatalogue.Submit(); err != nil {
		return fmt.Errorf("submitting catalogue write query: %w", err)
	}

	return nil
}

// OpenArrayForWriteAt opens a TileDB array in WRITE mode tagged with an explicit millisecond timestamp.
func OpenArrayForWriteAt(ctx *tiledb.Context, arrayURI string, timestamp uint64) (*tiledb.Array, error) {
	array, err := tiledb.NewArray(ctx, arrayURI)
	if err != nil {
		return nil, fmt.Errorf("failed creating array object for %s: %w", arrayURI, err)
	}

	err = array.OpenWithOptions(
		tiledb.TILEDB_WRITE,
		tiledb.WithStartTimestamp(timestamp),
		tiledb.WithEndTimestamp(timestamp),
	)
	if err != nil {
		array.Free()
		return nil, fmt.Errorf("failed opening array %s in WRITE mode at timestamp %d: %w", arrayURI, timestamp, err)
	}

	return array, nil
}
