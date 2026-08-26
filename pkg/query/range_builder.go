package query

import (
	"fmt"
	"sort"

	"github.com/paulmach/orb/geojson"
	rhealpix "github.com/sixy6e/go-rhealpix"
	"github.com/sixy6e/go-rhealpix/rhealpixorb"
)

// ----------------------------------------------------------------------------
// 64-Bit Range Generation (Res 0..14)
// ----------------------------------------------------------------------------

// BuildTileDBRanges64 decomposes an input GeoJSON query geometry into coarse tiles at coarseRes,
// and constructs a complete set of TileDB query ranges for compacted multi-resolution arrays:
//  1. Descendant Ranges: [FirstChild(targetRes), LastChild(targetRes)] covering cells at targetRes.
//  2. Ancestor Ranges: Point ranges [Parent(k), Parent(k)] for k in [0..coarseRes-1] catching
//     large interior compacted cells.
func BuildTileDBRanges64(
	el *rhealpix.Ellipsoid,
	geo geojson.Geometry,
	coarseRes, targetRes uint8,
) ([]rhealpixorb.Uint64Range, error) {
	if coarseRes > targetRes {
		return nil, fmt.Errorf("coarseRes (%d) cannot be greater than targetRes (%d)", coarseRes, targetRes)
	}

	coarseCells, err := rhealpixorb.STACGeometryToCompactedCells(el, geo, coarseRes)
	if err != nil {
		return nil, fmt.Errorf("coarse geometry decomposition failed: %w", err)
	}

	seenAncestors := make(map[uint64]bool)
	var ranges []rhealpixorb.Uint64Range

	for _, cell := range coarseCells {
		// Descendant Range [Min, Max] at targetRes
		// (i.e. First and Last child in descendant cell at res)
		minCell, maxCell := cell.SubtreeRange(targetRes)
		ranges = append(ranges, rhealpixorb.Uint64Range{
			Min: minCell.Uint64(),
			Max: maxCell.Uint64(),
		})

		// Ancestor Point Ranges covering Res 0 through (coarseRes - 1)
		ancestors, err := cell.Ancestors()
		if err != nil {
			return nil, fmt.Errorf("failed retrieving ancestors for cell %s: %w", cell.Hex(), err)
		}

		for _, anc := range ancestors {
			val := anc.Uint64()
			if !seenAncestors[val] {
				seenAncestors[val] = true
				ranges = append(ranges, rhealpixorb.Uint64Range{
					Min: val,
					Max: val,
				})
			}
		}
	}

	// sort ranges numerically
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Min == ranges[j].Min {
			return ranges[i].Max < ranges[j].Max
		}
		return ranges[i].Min < ranges[j].Min
	})

	return ranges, nil
}

// ----------------------------------------------------------------------------
// 128-Bit Range Generation (Res 0..30)
// ----------------------------------------------------------------------------

// BuildTileDBRanges128 constructs query ranges for 128-bit rHEALPix arrays (e.g. LiDAR, Bathymetry)
// using 128-bit bit-packed registers.
func BuildTileDBRanges128(
	el *rhealpix.Ellipsoid,
	geo geojson.Geometry,
	coarseRes, targetRes uint8,
) ([]rhealpixorb.Uint128Range, error) {
	if coarseRes > targetRes {
		return nil, fmt.Errorf("coarseRes (%d) cannot be greater than targetRes (%d)", coarseRes, targetRes)
	}

	// unpack coarse cells from GeoJSON using 128-bit decomposition
	coarseCells, err := rhealpixorb.STACGeometryToCompactedCells128(el, geo, coarseRes)
	if err != nil {
		return nil, fmt.Errorf("coarse 128-bit geometry decomposition failed: %w", err)
	}

	type key128 struct {
		High uint64
		Low  uint64
	}
	seenAncestors := make(map[key128]bool)
	var ranges []rhealpixorb.Uint128Range

	for _, cell := range coarseCells {
		// Descendant Range [Min, Max] at targetRes
		// (i.e. First and Last child in descendant cell at res)
		minCell, maxCell := cell.SubtreeRange(targetRes)
		minHigh, minLow := minCell.Uint64s()
		maxHigh, maxLow := maxCell.Uint64s()

		ranges = append(ranges, rhealpixorb.Uint128Range{
			MinHigh: minHigh,
			MinLow:  minLow,
			MaxHigh: maxHigh,
			MaxLow:  maxLow,
		})

		// Ancestor Point Ranges covering Res 0 through (coarseRes - 1)
		ancestors, err := cell.Ancestors()
		if err != nil {
			return nil, fmt.Errorf("failed retrieving 128-bit ancestors: %w", err)
		}

		for _, anc := range ancestors {
			hi, lo := anc.Uint64s()
			k := key128{High: hi, Low: lo}
			if !seenAncestors[k] {
				seenAncestors[k] = true
				ranges = append(ranges, rhealpixorb.Uint128Range{
					MinHigh: hi,
					MinLow:  lo,
					MaxHigh: hi,
					MaxLow:  lo,
				})
			}
		}
	}

	// sort 128-bit ranges numerically by High word, then Low word
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].MinHigh == ranges[j].MinHigh {
			return ranges[i].MinLow < ranges[j].MinLow
		}
		return ranges[i].MinHigh < ranges[j].MinHigh
	})

	return ranges, nil
}
