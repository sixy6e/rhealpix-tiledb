package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	rhealpix "github.com/sixy6e/go-rhealpix"
	"github.com/sixy6e/go-rhealpix/rhealpixorb"
	"github.com/sixy6e/rhealpix-tiledb/pkg/pipeline"
	"github.com/sixy6e/rhealpix-tiledb/pkg/stac"
	"github.com/sixy6e/rhealpix-tiledb/pkg/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type S5CmdListing struct {
	Key string `json:"key"`
}

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Stream STAC manifest scenes into a product group",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// bind flags at runtime so CLI flags override config.yaml values
		return viper.BindPFlags(cmd.Flags())
	},
	RunE: runIngest,
}

func init() {
	ingestCmd.Flags().String("manifest", "", "Path to manifest file with STAC JSON URIs (Required)")
	ingestCmd.Flags().String("catalogue", "data/catalogue", "Root TileDB catalogue URI")
	ingestCmd.Flags().String("product", "sentinel-2", "Product group name")
	ingestCmd.Flags().Bool("init", false, "Purge and recreate product group schemas")
	ingestCmd.Flags().Uint("resolution", 13, "rHEALPix resolution depth (12 or 13)")
	ingestCmd.Flags().Int("batch_size", 200, "Flush batch size")
	ingestCmd.Flags().Int("workers", 0, "Worker pool concurrency")

	rootCmd.AddCommand(ingestCmd)
}

func runIngest(cmd *cobra.Command, args []string) error {
	manifestPath := viper.GetString("manifest")
	if manifestPath == "" {
		return fmt.Errorf("flag --manifest is required")
	}

	catalogueRoot := viper.GetString("catalogue")
	productName := viper.GetString("product")
	initMode := viper.GetBool("init")
	targetRes := uint8(viper.GetUint("resolution"))
	batchSize := viper.GetInt("batch_size")
	workers := viper.GetInt("workers")

	var readCfg, writeCfg storage.ContextConfig
	_ = viper.UnmarshalKey("read_tiledb", &readCfg)
	_ = viper.UnmarshalKey("write_tiledb", &writeCfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	t0 := time.Now()

	// construct dual TileDB contexts (READ S3 vs WRITE S3)
	dualCtx, err := storage.NewDualContext(readCfg, writeCfg)
	if err != nil {
		return fmt.Errorf("failed setting up dual tiledb context: %w", err)
	}
	defer dualCtx.Free()

	// ensure root catalogue directory and group exist across local/S3 VFS
	isDir, err := dualCtx.WriteVFS.IsDir(catalogueRoot)
	if err != nil || !isDir {
		fmt.Printf("Creating root catalogue group at: %s...\n", catalogueRoot)
		_ = dualCtx.WriteVFS.CreateDir(catalogueRoot)
		_ = tiledb.CreateGroup(dualCtx.WriteCtx, catalogueRoot)
	}

	productURI := storage.JoinURI(catalogueRoot, productName)
	cellsURI := storage.JoinURI(productURI, "footprint-cells.tiledb")
	catalogueURI := storage.JoinURI(productURI, "scene-catalogue.tiledb")

	if initMode {
		fmt.Printf("RESET MODE: Purging product '%s' under catalogue '%s'...\n", productName, catalogueRoot)
		productURI, err = storage.PurgeAndCreateProductGroup(dualCtx.WriteCtx, dualCtx.WriteVFS, catalogueRoot, productName)
		if err != nil {
			return fmt.Errorf("purging product group: %w", err)
		}

		err = storage.PurgeAndCreateArray(dualCtx.WriteCtx, dualCtx.WriteVFS, productURI, "footprint-cells.tiledb", "footprint-cells", storage.CreateFootprintCellsSchema)
		if err != nil {
			return fmt.Errorf("creating cells array: %w", err)
		}

		err = storage.PurgeAndCreateArray(dualCtx.WriteCtx, dualCtx.WriteVFS, productURI, "scene-catalogue.tiledb", "scene-catalogue", storage.CreateSceneCatalogueSchema)
		if err != nil {
			return fmt.Errorf("creating catalogue array: %w", err)
		}
	} else {
		_ = storage.EnsureProductGroup(dualCtx.WriteCtx, dualCtx.WriteVFS, productURI)
	}

	// setup pipeline engine
	cfg := pipeline.DefaultConfig(targetRes)
	cfg.BatchSize = batchSize
	if workers > 0 {
		cfg.NumWorkers = workers
	}

	// just for prototype logging purposes
	fmt.Printf("Starting Ingestion | Product: %s | Workers: %d | BatchSize: %d | Res: %d\n",
		productName, cfg.NumWorkers, cfg.BatchSize, cfg.TargetResolution)

	el := rhealpix.NewWGS84()

	// READ worker callback
	workerFn := func(c context.Context, uri string, res uint8) (*pipeline.IngestRecord, error) {
		item, rawBytes, err := stac.ReadAndCleanSTACStream(dualCtx.ReadVFS, uri)
		if err != nil {
			return nil, err
		}

		dt, err := time.Parse(time.RFC3339, item.Properties.Datetime)
		if err != nil {
			dt = time.Now()
		}

		uHigh, uLow, err := stac.ParseUUIDToUint64Pair(item.ID)
		if err != nil {
			return nil, err
		}

		compactedCells, err := rhealpixorb.STACGeometryToCompactedCells(el, item.Geometry, res)
		if err != nil {
			return nil, err
		}

		return &pipeline.IngestRecord{
			UUIDHigh:       uHigh,
			UUIDLow:        uLow,
			SceneID:        item.ID,
			Datetime:       dt,
			CloudCover:     item.Properties.CloudCover,
			CompactedCells: compactedCells,
			STACJSON:       rawBytes,
		}, nil
	}

	// flusher callback
	var totalIngested2 int64
	totalIngested2 = 0
	flushFn := func(c context.Context, records []*pipeline.IngestRecord) error {
		tFlush := time.Now()
		err := storage.WriteDualArrayBatch(c, dualCtx.WriteCtx, cellsURI, catalogueURI, records)
		if err != nil {
			return err
		}

		totalIngested2 += int64(len(records))
		fmt.Printf("[%s] Flushed batch of %d records to TileDB (Total: %d | Duration: %v)\n",
			time.Now().Format("15:04:05"), len(records), totalIngested2, time.Since(tFlush))
		return nil
	}

	engine, err := pipeline.NewEngine(cfg, workerFn, flushFn)
	if err != nil {
		return err
	}

	engine.Start(ctx)

	go func() {
		defer engine.CloseInput()
		manifestFile, err := os.Open(manifestPath)
		if err != nil {
			return
		}
		defer manifestFile.Close()

		scanner := bufio.NewScanner(manifestFile)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
				line := scanner.Bytes()
				if len(line) == 0 {
					continue
				}
				var entry S5CmdListing
				if err := json.Unmarshal(line, &entry); err == nil && entry.Key != "" {
					_ = engine.Submit(ctx, entry.Key)
				}
			}
		}
	}()

	totalIngested, err := engine.RunFlusher(ctx)
	if err != nil {
		fmt.Printf("Pipeline warning: %v\n", err)
	}

	fmt.Printf("\n Ingestion Complete! Scenes Ingested: %d | Elapsed: %v\n", totalIngested, time.Since(t0))
	return nil
}
