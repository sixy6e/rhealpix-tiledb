package storage

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

type DualContext struct {
	ReadCtx  *tiledb.Context
	ReadVFS  *tiledb.VFS
	WriteCtx *tiledb.Context
	WriteVFS *tiledb.VFS
}

type ContextConfig struct {
	// Path to native TileDB .cfg file
	ConfigFile string `mapstructure:"config_file"`
	// Key-value pairs (vfs.s3.region, etc.)
	Options map[string]string `mapstructure:"options"`
}

// BuildConfig creates a tiledb.Config by loading an optional config file
// and overriding/augmenting it with inline map options.
func BuildConfig(cfg ContextConfig) (*tiledb.Config, error) {
	var config *tiledb.Config
	var err error

	// load from native TileDB config file if provided
	if cfg.ConfigFile != "" {
		config, err = tiledb.LoadConfig(cfg.ConfigFile)
		if err != nil {
			return nil, fmt.Errorf("loading TileDB config file '%s': %w", cfg.ConfigFile, err)
		}
	} else {
		config, err = tiledb.NewConfig()
		if err != nil {
			return nil, fmt.Errorf("creating TileDB config: %w", err)
		}
	}

	// set any dynamic key-value options passed via viper config
	for k, v := range cfg.Options {
		if v != "" {
			if err := config.Set(k, v); err != nil {
				return nil, fmt.Errorf("setting TileDB config '%s=%s': %w", k, v, err)
			}
		}
	}

	return config, nil
}

// NewDualContext builds isolated contexts using flexible TileDB configurations.
func NewDualContext(readCfg, writeCfg ContextConfig) (*DualContext, error) {
	// build READ TileDB config
	rCfg, err := BuildConfig(readCfg)
	if err != nil {
		return nil, fmt.Errorf("read config error: %w", err)
	}
	defer rCfg.Free()

	readCtx, err := tiledb.NewContext(rCfg)
	if err != nil {
		return nil, fmt.Errorf("creating read context: %w", err)
	}

	readVFS, err := tiledb.NewVFS(readCtx, rCfg)
	if err != nil {
		readCtx.Free()
		return nil, fmt.Errorf("creating read VFS: %w", err)
	}

	// build WRITE TileDB config
	wCfg, err := BuildConfig(writeCfg)
	if err != nil {
		readVFS.Free()
		readCtx.Free()
		return nil, fmt.Errorf("write config error: %w", err)
	}
	defer wCfg.Free()

	writeCtx, err := tiledb.NewContext(wCfg)
	if err != nil {
		readVFS.Free()
		readCtx.Free()
		return nil, fmt.Errorf("creating write context: %w", err)
	}

	writeVFS, err := tiledb.NewVFS(writeCtx, wCfg)
	if err != nil {
		writeCtx.Free()
		readVFS.Free()
		readCtx.Free()
		return nil, fmt.Errorf("creating write VFS: %w", err)
	}

	return &DualContext{
		ReadCtx:  readCtx,
		ReadVFS:  readVFS,
		WriteCtx: writeCtx,
		WriteVFS: writeVFS,
	}, nil
}

func (dc *DualContext) Free() {
	if dc.ReadVFS != nil {
		dc.ReadVFS.Free()
	}
	if dc.ReadCtx != nil {
		dc.ReadCtx.Free()
	}
	if dc.WriteVFS != nil {
		dc.WriteVFS.Free()
	}
	if dc.WriteCtx != nil {
		dc.WriteCtx.Free()
	}
}
