package storage

import (
	"fmt"
	"path"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// memberExists checks if a member name or URI exists by opening the group in READ mode.
func memberExists(ctx *tiledb.Context, groupURI string, target string) bool {
	group, err := tiledb.NewGroup(ctx, groupURI)
	if err != nil {
		return false
	}
	defer group.Free()

	if err := group.Open(tiledb.TILEDB_READ); err != nil {
		return false
	}
	defer group.Close()

	count, err := group.GetMemberCount()
	if err != nil {
		return false
	}

	for i := uint64(0); i < count; i++ {
		uri, name, _, err := group.GetMemberFromIndex(i)
		if err == nil {
			if name == target || uri == target || path.Base(uri) == target {
				return true
			}
		}
	}
	return false
}

// removeMemberIfExists inspects in READ mode first, then opens in WRITE mode to remove if found.
func removeMemberIfExists(ctx *tiledb.Context, groupURI string, target string) {
	if !memberExists(ctx, groupURI, target) {
		return
	}

	group, err := tiledb.NewGroup(ctx, groupURI)
	if err != nil {
		return
	}
	defer group.Free()

	if err := group.Open(tiledb.TILEDB_WRITE); err == nil {
		_ = group.RemoveMember(target)
		group.Close()
	}
}

// PurgeAndCreateArray checks if an array exists at arrayURI.
// If part of a product group, it unregisters the member (if present),
// purges the array directory via VFS, creates a new schema, and re-registers the member.
func PurgeAndCreateArray(
	ctx *tiledb.Context,
	vfs *tiledb.VFS,
	productGroupURI string,
	arrayName string, // e.g. "footprint-cells.tiledb"
	memberName string, // e.g. "footprint-cells"
	schemaFn SchemaBuilderFunc,
) error {
	arrayURI := JoinURI(productGroupURI, arrayName)

	// remove member from product group ONLY if it currently exists
	removeMemberIfExists(ctx, productGroupURI, memberName)
	removeMemberIfExists(ctx, productGroupURI, arrayName)

	// remove array directory via VFS if present
	isDir, err := vfs.IsDir(arrayURI)
	if err == nil && isDir {
		fmt.Printf("Purging existing TileDB array at: %s\n", arrayURI)
		if err := vfs.RemoveDir(arrayURI); err != nil {
			return fmt.Errorf("failed removing existing VFS array directory %s: %w", arrayURI, err)
		}
	}

	// build Array Schema
	schema, err := schemaFn(ctx)
	if err != nil {
		return fmt.Errorf("failed creating schema for %s: %w", arrayURI, err)
	}
	defer schema.Free()

	// create fresh Array
	array, err := tiledb.NewArray(ctx, arrayURI)
	if err != nil {
		return fmt.Errorf("failed initialising array object for %s: %w", arrayURI, err)
	}
	defer array.Free()

	if err := array.Create(schema); err != nil {
		return fmt.Errorf("failed creating TileDB array at %s: %w", arrayURI, err)
	}

	// re-register member back into product group (relative=true)
	group, err := tiledb.NewGroup(ctx, productGroupURI)
	if err == nil {
		if err := group.Open(tiledb.TILEDB_WRITE); err == nil {
			_ = group.AddMember(arrayName, memberName, true)
			group.Close()
		}
		group.Free()
	}

	return nil
}

// PurgeAndCreateProductGroup completely purges a product group from catalogue root.
func PurgeAndCreateProductGroup(
	ctx *tiledb.Context,
	vfs *tiledb.VFS,
	catalogueRootURI string,
	productName string, // e.g. "landsat-8"
) (string, error) {
	productURI := JoinURI(catalogueRootURI, productName)

	// remove product group member from root catalogue group ONLY if present
	removeMemberIfExists(ctx, catalogueRootURI, productName)

	// delete entire product group directory via VFS
	isDir, err := vfs.IsDir(productURI)
	if err == nil && isDir {
		fmt.Printf("Purging product directory at: %s\n", productURI)
		if err := vfs.RemoveDir(productURI); err != nil {
			return "", fmt.Errorf("failed purging product directory %s: %w", productURI, err)
		}
	}

	// re-create product group
	if err := tiledb.CreateGroup(ctx, productURI); err != nil {
		_ = vfs.CreateDir(productURI)
	}

	// re-add product group to root catalogue group (relative=true)
	rootGroup, err := tiledb.NewGroup(ctx, catalogueRootURI)
	if err == nil {
		if err := rootGroup.Open(tiledb.TILEDB_WRITE); err == nil {
			_ = rootGroup.AddMember(productName, productName, true)
			rootGroup.Close()
		}
		rootGroup.Free()
	}

	return productURI, nil
}
