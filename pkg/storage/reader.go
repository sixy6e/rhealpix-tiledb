package storage

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

type UUIDPair struct {
	High uint64
	Low  uint64
}

// Stage1QueryCells executes a spatio-temporal range search against footprint-cells.tiledb.
func Stage1QueryCells(
	tdbCtx *tiledb.Context,
	cellsURI string,
	queryRanges [][2]uint64,
	startNS, endNS int64,
) ([]UUIDPair, error) {
	array, err := tiledb.NewArray(tdbCtx, cellsURI)
	if err != nil {
		return nil, fmt.Errorf("opening cells array: %w", err)
	}
	defer array.Free()

	if err := array.Open(tiledb.TILEDB_READ); err != nil {
		return nil, fmt.Errorf("opening cells array for read: %w", err)
	}
	defer array.Close()

	query, err := tiledb.NewQuery(tdbCtx, array)
	if err != nil {
		return nil, fmt.Errorf("creating query: %w", err)
	}
	defer query.Free()

	subarray, err := array.NewSubarray()
	if err != nil {
		return nil, fmt.Errorf("creating subarray: %w", err)
	}
	defer subarray.Free()

	if err := subarray.AddRangeByName("datetime", tiledb.MakeRange(startNS, endNS)); err != nil {
		return nil, fmt.Errorf("setting datetime range: %w", err)
	}

	for _, r := range queryRanges {
		if err := subarray.AddRangeByName("cell_id", tiledb.MakeRange(r[0], r[1])); err != nil {
			return nil, fmt.Errorf("setting cell_id range: %w", err)
		}
	}

	if err := query.SetSubarray(subarray); err != nil {
		return nil, fmt.Errorf("setting query subarray: %w", err)
	}

	const maxResults = 500000
	uuidHighs := make([]uint64, maxResults)
	uuidLows := make([]uint64, maxResults)

	if _, err := query.SetDataBuffer("uuid_high", uuidHighs); err != nil {
		return nil, err
	}
	if _, err := query.SetDataBuffer("uuid_low", uuidLows); err != nil {
		return nil, err
	}

	if err := query.Submit(); err != nil {
		return nil, fmt.Errorf("submitting query: %w", err)
	}

	elements, err := query.ResultBufferElements()
	if err != nil {
		return nil, fmt.Errorf("getting num elements: %w", err)
	}
	numRead := elements["uuid_high"][1]

	uniqueUUIDs := make(map[UUIDPair]bool)
	var results []UUIDPair

	for i := uint64(0); i < numRead; i++ {
		pair := UUIDPair{High: uuidHighs[i], Low: uuidLows[i]}
		if !uniqueUUIDs[pair] {
			uniqueUUIDs[pair] = true
			results = append(results, pair)
		}
	}

	return results, nil
}

// Stage2FetchCatalogue retrieves full STAC JSON strings by UUID pairs.
func Stage2FetchCatalogue(tdbCtx *tiledb.Context, catalogueURI string, uuids []UUIDPair) ([]string, error) {
	if len(uuids) == 0 {
		return nil, nil
	}

	array, err := tiledb.NewArray(tdbCtx, catalogueURI)
	if err != nil {
		return nil, fmt.Errorf("opening catalogue array: %w", err)
	}
	defer array.Free()

	if err := array.Open(tiledb.TILEDB_READ); err != nil {
		return nil, fmt.Errorf("opening catalogue array for read: %w", err)
	}
	defer array.Close()

	query, err := tiledb.NewQuery(tdbCtx, array)
	if err != nil {
		return nil, fmt.Errorf("creating catalogue query: %w", err)
	}
	defer query.Free()

	subarray, err := array.NewSubarray()
	if err != nil {
		return nil, fmt.Errorf("creating catalogue subarray: %w", err)
	}
	defer subarray.Free()

	for _, pair := range uuids {
		_ = subarray.AddRangeByName("uuid_high", tiledb.MakeRange(pair.High, pair.High))
		_ = subarray.AddRangeByName("uuid_low", tiledb.MakeRange(pair.Low, pair.Low))
	}

	if err := query.SetSubarray(subarray); err != nil {
		return nil, fmt.Errorf("setting catalogue query subarray: %w", err)
	}

	const maxDataBytes = 10 * 1024 * 1024
	const maxOffsets = 1000

	stacData := make([]byte, maxDataBytes)
	stacOffsets := make([]uint64, maxOffsets)

	if _, err := query.SetDataBuffer("stac_json", stacData); err != nil {
		return nil, err
	}
	if _, err := query.SetOffsetsBuffer("stac_json", stacOffsets); err != nil {
		return nil, err
	}

	if err := query.Submit(); err != nil {
		return nil, fmt.Errorf("submitting catalogue query: %w", err)
	}

	elements, err := query.ResultBufferElements()
	if err != nil {
		return nil, fmt.Errorf("getting catalogue buffer elements: %w", err)
	}
	numRead := elements["stac_json"][0]

	var stacJSONs []string
	for i := uint64(0); i < numRead; i++ {
		start := stacOffsets[i]
		var end uint64
		if i+1 < numRead {
			end = stacOffsets[i+1]
		} else {
			end = elements["stac_json"][1]
		}
		stacJSONs = append(stacJSONs, string(stacData[start:end]))
	}

	return stacJSONs, nil
}
