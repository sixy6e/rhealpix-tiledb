package main

import (
	"fmt"

	"github.com/sixy6e/rhealpix-tiledb/pkg/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var catalogueCmd = &cobra.Command{
	Use:   "catalogue",
	Short: "Inspect, list, and maintain TileDB DGGS catalogue product groups",
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all registered products and array endpoints in a catalogue root",
	RunE:  runCatalogueList,
}

func init() {
	listCmd.Flags().String("catalogue", "data/catalogue", "Root TileDB catalogue URI")
	_ = viper.BindPFlag("catalogue", listCmd.Flags().Lookup("catalogue"))

	catalogueCmd.AddCommand(listCmd)
	rootCmd.AddCommand(catalogueCmd)
}

func runCatalogueList(cmd *cobra.Command, args []string) error {
	catalogueRoot := viper.GetString("catalogue")

	var readCfg storage.ContextConfig
	_ = viper.UnmarshalKey("read_tiledb", &readCfg)

	// context for catalog discovery (uses READ context/options)
	dualCtx, err := storage.NewDualContext(readCfg, readCfg)
	if err != nil {
		return fmt.Errorf("setting up TileDB context: %w", err)
	}
	defer dualCtx.Free()

	products, err := storage.ListCatalogueProducts(dualCtx.ReadCtx, catalogueRoot)
	if err != nil {
		return fmt.Errorf("failed listing products: %w", err)
	}

	fmt.Printf("TileDB DGGS Catalogue Root: %s\n", catalogueRoot)
	fmt.Println("------------------------------------------------------------------")
	if len(products) == 0 {
		fmt.Println("  (No registered products found in group)")
		return nil
	}

	for _, p := range products {
		fmt.Printf("  • Product: %s\n", p.Name)
		fmt.Printf("    ├── Root URI:      %s\n", p.URI)
		fmt.Printf("    ├── Cells Array:   %s\n", p.CellsURI)
		fmt.Printf("    └── Catalogue Array: %s\n", p.CatalogueURI)
		fmt.Println()
	}

	return nil
}
