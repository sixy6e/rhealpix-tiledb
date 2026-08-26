package pipeline

import (
	"fmt"
	"runtime"
)

// Config defines execution parameters for the streaming ingestion pipeline.
type Config struct {
	// NumWorkers sets the number of concurrent processing workers.
	// Defaults to runtime.NumCPU() to align with hardware threads.
	NumWorkers int

	// BatchSize sets the number of processed records aggregated before a TileDB flush.
	// 150-200 to keep memory bounded < 4GB (largely depends on TargetResolution).
	BatchSize int

	// JobQueueSize sets the depth of the incoming URI channel.
	// Enforces backpressure on the producer reading from manifests.
	JobQueueSize int

	// ResultQueueSize sets the depth of the processed records channel.
	// Enforces backpressure on workers when TileDB is actively flushing to disk.
	ResultQueueSize int

	// TargetResolution specifies the rHEALPix resolution level for geometry decomposition (e.g. 12 or 13).
	TargetResolution uint8
}

// DefaultConfig returns an auto-tuned configuration based on available CPU resources.
func DefaultConfig(targetRes uint8) Config {
	cpus := runtime.NumCPU()
	return Config{
		NumWorkers:       cpus,     // Match physical/logical thread count
		BatchSize:        200,      // ~12-16M cell keys per flush block
		JobQueueSize:     cpus * 2, // Controlled job buffer
		ResultQueueSize:  cpus,     // Direct backpressure to prevent memory runaway
		TargetResolution: targetRes,
	}
}

// Validate ensures all configuration parameters are positive and non-zero.
func (c Config) Validate() error {
	if c.NumWorkers <= 0 {
		return fmt.Errorf("numWorkers must be > 0 (got %d)", c.NumWorkers)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batchSize must be > 0 (got %d)", c.BatchSize)
	}
	if c.JobQueueSize <= 0 {
		return fmt.Errorf("jobQueueSize must be > 0 (got %d)", c.JobQueueSize)
	}
	if c.ResultQueueSize <= 0 {
		return fmt.Errorf("resultQueueSize must be > 0 (got %d)", c.ResultQueueSize)
	}
	return nil
}
