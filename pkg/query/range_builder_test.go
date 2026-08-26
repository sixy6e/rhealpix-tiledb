package query

import (
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	rhealpix "github.com/sixy6e/go-rhealpix"
)

func TestBuildTileDBRanges64(t *testing.T) {
	el := rhealpix.NewWGS84()

	// small Canberra bounding box
	bbox := orb.Bound{
		Min: orb.Point{148.9, -35.5},
		Max: orb.Point{149.3, -35.1},
	}
	poly := bbox.ToPolygon()
	geo := geojson.NewGeometry(poly)

	coarseRes := uint8(7)
	targetRes := uint8(12)

	ranges, err := BuildTileDBRanges64(el, *geo, coarseRes, targetRes)
	if err != nil {
		t.Fatalf("BuildTileDBRanges64 failed: %v", err)
	}

	if len(ranges) == 0 {
		t.Fatal("expected generated ranges, got 0")
	}

	// verify range sorting
	for i := 1; i < len(ranges); i++ {
		if ranges[i].Min < ranges[i-1].Min {
			t.Errorf("ranges out of order at index %d: %d < %d", i, ranges[i].Min, ranges[i-1].Min)
		}
	}
}
