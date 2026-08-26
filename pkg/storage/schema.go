package storage

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// SchemaBuilderFunc defines a function signature that constructs a TileDB ArraySchema.
type SchemaBuilderFunc func(ctx *tiledb.Context) (*tiledb.ArraySchema, error)

// CreateFootprintCellsSchema builds the schema for footprint-cells.tiledb:
// Dimensions: cell_id (uint64), datetime (int64 nanoseconds)
// Attributes: uuid_high (uint64), uuid_low (uint64)
func CreateFootprintCellsSchema(ctx *tiledb.Context) (*tiledb.ArraySchema, error) {
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	if err != nil {
		return nil, fmt.Errorf("creating array schema: %w", err)
	}

	domain, err := tiledb.NewDomain(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating domain: %w", err)
	}

	// spatial dimension (CellID64) TODO: cater for CellID128
	tileExtentCellID := uint64(100000)
	maxDomainCellID := ^uint64(0) - tileExtentCellID
	dimCellID, err := tiledb.NewDimension(ctx, "cell_id", tiledb.TILEDB_UINT64, []uint64{0, maxDomainCellID}, tileExtentCellID)
	if err != nil {
		return nil, fmt.Errorf("creating cell_id dimension: %w", err)
	}

	// temporal dimension (Unix Nanoseconds) (24hr 1-Day)
	dimDatetime, err := tiledb.NewDimension(ctx, "datetime", tiledb.TILEDB_DATETIME_NS, []int64{0, 4102444800000000000}, int64(86400000000000))
	if err != nil {
		return nil, fmt.Errorf("creating datetime dimension: %w", err)
	}

	if err := domain.AddDimensions(dimCellID, dimDatetime); err != nil {
		return nil, fmt.Errorf("adding dimensions: %w", err)
	}

	if err := schema.SetDomain(domain); err != nil {
		return nil, fmt.Errorf("setting domain: %w", err)
	}

	if err := schema.SetCellOrder(tiledb.TILEDB_HILBERT); err != nil {
		return nil, fmt.Errorf("setting hilbert cell order: %w", err)
	}

	// attributes: UUID split across High and Low uint64 registers
	attrHigh, err := tiledb.NewAttribute(ctx, "uuid_high", tiledb.TILEDB_UINT64)
	if err != nil {
		return nil, fmt.Errorf("creating uuid_high attr: %w", err)
	}
	attrLow, err := tiledb.NewAttribute(ctx, "uuid_low", tiledb.TILEDB_UINT64)
	if err != nil {
		return nil, fmt.Errorf("creating uuid_low attr: %w", err)
	}

	// compression filter pipeline: ByteShuffle + ZSTD Level 16
	zstdFilter, _ := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_ZSTD)
	_ = zstdFilter.SetOption(tiledb.TILEDB_COMPRESSION_LEVEL, int32(16))

	byteShuffle, _ := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_BITSHUFFLE)

	filterList, _ := tiledb.NewFilterList(ctx)
	_ = filterList.AddFilter(byteShuffle)
	_ = filterList.AddFilter(zstdFilter)

	_ = attrHigh.SetFilterList(filterList)
	_ = attrLow.SetFilterList(filterList)

	if err := schema.AddAttributes(attrHigh, attrLow); err != nil {
		return nil, fmt.Errorf("adding attributes: %w", err)
	}

	return schema, nil
}

// CreateSceneCatalogueSchema builds the schema for scene-catalogue.tiledb:
// Dimensions: uuid_high (uint64), uuid_low (uint64)
// Attributes: stac_json (variable-length string)
func CreateSceneCatalogueSchema(ctx *tiledb.Context) (*tiledb.ArraySchema, error) {
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	if err != nil {
		return nil, fmt.Errorf("creating catalogue array schema: %w", err)
	}

	domain, err := tiledb.NewDomain(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating domain: %w", err)
	}

	tileExtentUUID := uint64(10000)
	maxDomainUUID := ^uint64(0) - tileExtentUUID
	dimHigh, err := tiledb.NewDimension(ctx, "uuid_high", tiledb.TILEDB_UINT64, []uint64{0, maxDomainUUID}, tileExtentUUID)
	if err != nil {
		return nil, fmt.Errorf("creating uuid_high dimension: %w", err)
	}
	dimLow, err := tiledb.NewDimension(ctx, "uuid_low", tiledb.TILEDB_UINT64, []uint64{0, maxDomainUUID}, tileExtentUUID)
	if err != nil {
		return nil, fmt.Errorf("creating uuid_low dimension: %w", err)
	}

	if err := domain.AddDimensions(dimHigh, dimLow); err != nil {
		return nil, fmt.Errorf("adding dimensions: %w", err)
	}
	if err := schema.SetDomain(domain); err != nil {
		return nil, fmt.Errorf("setting domain: %w", err)
	}

	// attribute: STAC JSON payload
	attrJSON, err := tiledb.NewAttribute(ctx, "stac_json", tiledb.TILEDB_CHAR)
	if err != nil {
		return nil, fmt.Errorf("creating stac_json attr: %w", err)
	}
	if err := attrJSON.SetCellValNum(tiledb.TILEDB_VAR_NUM); err != nil {
		return nil, fmt.Errorf("setting var_num on stac_json: %w", err)
	}

	// data compression filter: ZSTD Level 16
	zstdFilter, _ := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_ZSTD)
	_ = zstdFilter.SetOption(tiledb.TILEDB_COMPRESSION_LEVEL, int32(16))

	dataFilterList, _ := tiledb.NewFilterList(ctx)
	_ = dataFilterList.AddFilter(zstdFilter)
	_ = attrJSON.SetFilterList(dataFilterList)

	// offsets buffer compression filter: BitShuffle + ZSTD Level 16
	bitShuffle, _ := tiledb.NewFilter(ctx, tiledb.TILEDB_FILTER_BITSHUFFLE)
	offsetsFilterList, _ := tiledb.NewFilterList(ctx)
	_ = offsetsFilterList.AddFilter(bitShuffle)
	_ = offsetsFilterList.AddFilter(zstdFilter)
	_ = schema.SetOffsetsFilterList(offsetsFilterList)

	if err := schema.AddAttributes(attrJSON); err != nil {
		return nil, fmt.Errorf("adding stac_json attribute: %w", err)
	}

	return schema, nil
}
