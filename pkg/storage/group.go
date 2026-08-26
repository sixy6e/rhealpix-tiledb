package storage

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// EnsureProductGroup creates a TileDB Group at productURI and registers
// footprint-cells and scene-catalogue as child array members using relative paths.
func EnsureProductGroup(ctx *tiledb.Context, vfs *tiledb.VFS, productURI string) error {
	// ensure parent group directory exists
	isDir, err := vfs.IsDir(productURI)
	if err != nil || !isDir {
		if err := tiledb.CreateGroup(ctx, productURI); err != nil {
			_ = vfs.CreateDir(productURI)
		}
	}

	group, err := tiledb.NewGroup(ctx, productURI)
	if err != nil {
		return fmt.Errorf("initialising group %s: %w", productURI, err)
	}
	defer group.Free()

	if err := group.Open(tiledb.TILEDB_WRITE); err != nil {
		return fmt.Errorf("opening group %s for write: %w", productURI, err)
	}
	defer group.Close()

	// register member arrays using relative paths AddMember(uri, name, isRelative)
	_ = group.AddMember("footprint-cells.tiledb", "footprint-cells", true)
	_ = group.AddMember("scene-catalogue.tiledb", "scene-catalogue", true)

	return nil
}

// ProductCatalogue holds discovery info for a product inside the catalogue.
type ProductCatalogue struct {
	Name         string
	URI          string
	CellsURI     string
	CatalogueURI string
}

// ListCatalogueProducts inspects a TileDB Group root and returns all child products.
func ListCatalogueProducts(ctx *tiledb.Context, catalogueRootURI string) ([]ProductCatalogue, error) {
	group, err := tiledb.NewGroup(ctx, catalogueRootURI)
	if err != nil {
		return nil, fmt.Errorf("opening root group %s: %w", catalogueRootURI, err)
	}
	defer group.Free()

	if err := group.Open(tiledb.TILEDB_READ); err != nil {
		return nil, fmt.Errorf("opening group for read: %w", catalogueRootURI, err)
	}
	defer group.Close()

	count, err := group.GetMemberCount()
	if err != nil {
		return nil, fmt.Errorf("getting member count for %s: %w", catalogueRootURI, err)
	}

	var products []ProductCatalogue
	for i := uint64(0); i < count; i++ {
		// GetMemberFromIndex returns fully qualified absolute URI, name, objectType, err
		uri, name, _, err := group.GetMemberFromIndex(i)
		if err != nil {
			continue
		}

		if name == "" {
			name = uri
		}

		// the uri of the Group Member is already resolved by TileDB (e.g. "file:///.../sentinel-2" or "s3://.../sentinel-2")
		products = append(products, ProductCatalogue{
			Name:         name,
			URI:          uri,
			CellsURI:     uri + "/footprint-cells.tiledb",
			CatalogueURI: uri + "/scene-catalogue.tiledb",
		})
	}

	return products, nil
}
