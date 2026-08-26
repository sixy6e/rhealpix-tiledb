package main

import (
	"fmt"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	rhealpix "github.com/sixy6e/go-rhealpix"
	"github.com/sixy6e/rhealpix-tiledb/pkg/query"
	"github.com/sixy6e/rhealpix-tiledb/pkg/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Benchmark Stage 1 & Stage 2 spatio-temporal queries",
	RunE:  runQuery,
}

func init() {
	queryCmd.Flags().String("catalogue", "data/catalogue", "Root TileDB catalogue URI")
	queryCmd.Flags().String("product", "sentinel-2", "Product group name")
	queryCmd.Flags().Uint("coarse-res", 7, "Coarse tile resolution for range generation")
	queryCmd.Flags().Uint("target-res", 13, "Target index resolution matching array data density")
	queryCmd.Flags().String("start", "2025-01-01T00:00:00Z", "Start datetime (RFC3339)")
	queryCmd.Flags().String("end", "2025-12-31T23:59:59Z", "End datetime (RFC3339)")

	_ = viper.BindPFlag("catalogue", queryCmd.Flags().Lookup("catalogue"))
	_ = viper.BindPFlag("product", queryCmd.Flags().Lookup("product"))
	_ = viper.BindPFlag("coarse_res", queryCmd.Flags().Lookup("coarse-res"))
	_ = viper.BindPFlag("target_res", queryCmd.Flags().Lookup("target-res"))
	_ = viper.BindPFlag("start", queryCmd.Flags().Lookup("start"))
	_ = viper.BindPFlag("end", queryCmd.Flags().Lookup("end"))

	rootCmd.AddCommand(queryCmd)
}

func runQuery(cmd *cobra.Command, args []string) error {
	catalogueRoot := viper.GetString("catalogue")
	productName := viper.GetString("product")
	coarseRes := uint8(viper.GetUint("coarse_res"))
	targetRes := uint8(viper.GetUint("target_res"))

	startStr := viper.GetString("start")
	endStr := viper.GetString("end")

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return fmt.Errorf("invalid start time format: %w", err)
	}
	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		return fmt.Errorf("invalid end time format: %w", err)
	}

	var readCfg storage.ContextConfig
	_ = viper.UnmarshalKey("read_tiledb", &readCfg)

	// setup TileDB READ context
	dualCtx, err := storage.NewDualContext(readCfg, readCfg)
	if err != nil {
		return fmt.Errorf("setting up TileDB context: %w", err)
	}
	defer dualCtx.Free()

	// default query area: Canberra bounding box
	// TODO: enable bbox passing (or geom from which to convert to orb.Bound)
	bbox := orb.Bound{
		Min: orb.Point{148.9, -35.5},
		Max: orb.Point{149.3, -35.1},
	}
	geo := geojson.NewGeometry(bbox.ToPolygon())
	el := rhealpix.NewWGS84()

	fmt.Printf(" Generating rHEALPix Ranges [Coarse: %d → Target: %d]...\n", coarseRes, targetRes)
	t0 := time.Now()

	// build Subtree & Ancestor integer ranges
	ranges, err := query.BuildTileDBRanges64(el, *geo, coarseRes, targetRes)
	if err != nil {
		return fmt.Errorf("range generation failed: %w", err)
	}
	rangeTime := time.Since(t0)

	// format ranges into TileDB [Min, Max] slice format
	tdbRanges := make([][2]uint64, len(ranges))
	for i, r := range ranges {
		tdbRanges[i] = [2]uint64{r.Min, r.Max}
	}

	cellsURI := fmt.Sprintf("%s/%s/footprint-cells.tiledb", catalogueRoot, productName)
	catalogueURI := fmt.Sprintf("%s/%s/scene-catalogue.tiledb", catalogueRoot, productName)

	// stage 1 query: footprint cells spatio-temporal range search
	fmt.Printf(" Stage 1: Index Search against %s (%d ranges)...\n", cellsURI, len(tdbRanges))
	tStage1 := time.Now()

	uuids, err := storage.Stage1QueryCells(
		dualCtx.ReadCtx,
		cellsURI,
		tdbRanges,
		startTime.UnixNano(),
		endTime.UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("stage 1 query failed: %w", err)
	}
	stage1Duration := time.Since(tStage1)

	fmt.Printf("   Found %d matching scene UUIDs in %v\n", len(uuids), stage1Duration)

	// stage 2 query: STAC metadata payload fetch
	// TODO: enable query filters on attrs
	if len(uuids) > 0 {
		fmt.Printf(" Stage 2: Catalogue Payload Fetch against %s...\n", catalogueURI)
		tStage2 := time.Now()

		stacPayloads, err := storage.Stage2FetchCatalogue(dualCtx.ReadCtx, catalogueURI, uuids)
		if err != nil {
			return fmt.Errorf("stage 2 query failed: %w", err)
		}
		stage2Duration := time.Since(tStage2)

		fmt.Printf("   Fetched %d STAC JSON payloads in %v\n", len(stacPayloads), stage2Duration)
	}

	fmt.Printf("\n Benchmark Complete! Range Build: %v | Stage 1: %v | Total: %v\n",
		rangeTime, stage1Duration, time.Since(t0))

	return nil
}
