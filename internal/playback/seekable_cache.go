package playback

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bluenviron/mediamtx/internal/logger"
)

// inflightEntry tracks a single in-progress cache generation.
type inflightEntry struct {
	done chan struct{} // closed when generation finishes
	err  error        // set before done is closed
}

// SeekableCache is a production-grade file-based cache for seekable MP4 files.
//
// Key properties:
//   - Singleflight: concurrent requests for the same key wait on one generation.
//   - Atomic rename: the final file is visible only after a successful fsync + rename.
//   - Cleanup worker: removes expired files and enforces MaxSize via LRU (oldest-modified) eviction.
//   - Never serves a partially generated file.
type SeekableCache struct {
	Dir     string
	TTL     time.Duration
	MaxSize uint64
	Parent  logger.Writer

	mu       sync.Mutex
	inflight map[string]*inflightEntry

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Initialize starts the cache cleanup goroutine.
func (c *SeekableCache) Initialize() error {
	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return fmt.Errorf("cannot create seekable cache directory %q: %w", c.Dir, err)
	}

	// Quick write-test to catch permission problems at startup.
	probe := filepath.Join(c.Dir, ".write-test")
	if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
		return fmt.Errorf("seekable cache directory %q is not writable: %w", c.Dir, err)
	}
	os.Remove(probe) //nolint:errcheck

	c.inflight = make(map[string]*inflightEntry)
	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.wg.Add(1)
	go c.cleanupLoop()

	c.Log(logger.Info, "seekable cache initialized at %s (TTL=%s, max=%d bytes)", c.Dir, c.TTL, c.MaxSize)
	return nil
}

// Close stops the cleanup worker and waits for it to exit.
func (c *SeekableCache) Close() {
	c.cancel()
	c.wg.Wait()
}

// Log implements logger.Writer-compatible logging.
func (c *SeekableCache) Log(level logger.Level, format string, args ...any) {
	c.Parent.Log(level, "[seekable-cache] "+format, args...)
}

// FinalPath returns the path of a completed cache file for the given key.
func (c *SeekableCache) FinalPath(key string) string {
	return filepath.Join(c.Dir, key+".mp4")
}

// TempPath returns a unique temporary path for the given key.
func (c *SeekableCache) TempPath(key string) string {
	return filepath.Join(c.Dir, key+".tmp-"+fmt.Sprintf("%d", time.Now().UnixNano()))
}

// Get returns the path of the completed cache file for key.
//
// If the file does not exist yet, generate is called with a temp file path.
// generate must write a valid MP4 into that path; Get will then fsync, close
// and atomically rename it to the final path.
//
// Concurrent requests for the same key block until a single generation completes.
// Concurrent requests for different keys proceed independently.
func (c *SeekableCache) Get(key string, generate func(tmpPath string) error) (string, error) {
	finalPath := c.FinalPath(key)

	// Fast path: file already exists.
	if _, err := os.Stat(finalPath); err == nil {
		c.Log(logger.Info, "cache hit: %s", key)
		return finalPath, nil
	}

	// Slow path: need to generate (or wait for another goroutine to do so).
	c.mu.Lock()
	if entry, ok := c.inflight[key]; ok {
		// Another goroutine is already generating this key — wait for it.
		c.mu.Unlock()
		c.Log(logger.Debug, "waiting for in-flight generation: %s", key)
		<-entry.done
		if entry.err != nil {
			return "", entry.err
		}
		// Re-check that the file actually exists now.
		if _, err := os.Stat(finalPath); err != nil {
			return "", fmt.Errorf("cache file missing after generation: %w", err)
		}
		return finalPath, nil
	}

	// We are responsible for generating this key.
	entry := &inflightEntry{done: make(chan struct{})}
	c.inflight[key] = entry
	c.mu.Unlock()

	tmpPath := c.TempPath(key)
	genErr := c.runGenerate(key, tmpPath, finalPath, generate)

	entry.err = genErr
	close(entry.done)

	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()

	if genErr != nil {
		return "", genErr
	}
	return finalPath, nil
}

// runGenerate calls generate, then fsyncs + renames on success, or removes tmp on failure.
func (c *SeekableCache) runGenerate(key, tmpPath, finalPath string, generate func(string) error) error {
	start := time.Now()

	err := generate(tmpPath)
	if err != nil {
		os.Remove(tmpPath) //nolint:errcheck
		c.Log(logger.Error, "generation failed for %s: %v", key, err)
		return fmt.Errorf("generation failed: %w", err)
	}

	// fsync the tmp file before rename so the data is durable.
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0o644)
	if err == nil {
		if sErr := f.Sync(); sErr != nil {
			c.Log(logger.Warn, "fsync failed for %s: %v", tmpPath, sErr)
		}
		f.Close()
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath) //nolint:errcheck
		return fmt.Errorf("rename to final cache path failed: %w", err)
	}

	if fi, err := os.Stat(finalPath); err == nil {
		c.Log(logger.Info, "cache miss generated: %s in %s (%d bytes)",
			key, time.Since(start).Round(time.Millisecond), fi.Size())
	}
	return nil
}

// cleanupLoop runs periodic cache maintenance.
func (c *SeekableCache) cleanupLoop() {
	defer c.wg.Done()

	// Use half the TTL as the check interval, minimum 30 seconds.
	interval := c.TTL / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.cleanup()
		}
	}
}

type cacheFileInfo struct {
	path    string
	size    int64
	modTime time.Time
}

func (c *SeekableCache) cleanup() {
	now := time.Now()
	var files []cacheFileInfo
	var totalSize int64

	err := filepath.WalkDir(c.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		fi, err := d.Info()
		if err != nil {
			return nil
		}

		// Remove stale temporary files (from crashed/cancelled generations).
		if len(name) > 4 && name[len(name)-4:] != ".mp4" {
			age := now.Sub(fi.ModTime())
			if age > c.TTL {
				if rmErr := os.Remove(path); rmErr == nil {
					c.Log(logger.Info, "evicted stale temp file: %s (age=%s)", name, age.Round(time.Second))
				}
			}
			return nil
		}

		// Collect completed .mp4 files.
		if len(name) > 4 && name[len(name)-4:] == ".mp4" {
			age := now.Sub(fi.ModTime())
			if age > c.TTL {
				// Expired — remove immediately.
				if rmErr := os.Remove(path); rmErr == nil {
					c.Log(logger.Info, "evicted expired cache file: %s (age=%s)", name, age.Round(time.Second))
				}
				return nil
			}
			files = append(files, cacheFileInfo{
				path:    path,
				size:    fi.Size(),
				modTime: fi.ModTime(),
			})
			totalSize += fi.Size()
		}
		return nil
	})
	if err != nil {
		c.Log(logger.Warn, "cleanup walk error: %v", err)
	}

	// Enforce MaxSize: evict oldest (smallest modTime) files until under limit.
	if uint64(totalSize) > c.MaxSize {
		// Sort oldest first.
		sort.Slice(files, func(i, j int) bool {
			return files[i].modTime.Before(files[j].modTime)
		})
		for _, fi := range files {
			if uint64(totalSize) <= c.MaxSize {
				break
			}
			if rmErr := os.Remove(fi.path); rmErr == nil {
				totalSize -= fi.size
				c.Log(logger.Info, "evicted (size limit) cache file: %s (%d bytes)", filepath.Base(fi.path), fi.size)
			}
		}
	}
}
