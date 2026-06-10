package recorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type KeyframeIndexEntry struct {
	WallTime   time.Time `json:"wall_time"`
	Segment    string    `json:"segment"`
	MonoPTS    int64     `json:"mono_pts"`
	IsGapStart bool      `json:"is_gap_start"` // true if a silence/gap was detected before this keyframe
}

type KeyframeIndex struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

func (k *KeyframeIndex) Initialize(recordingDir string, streamName string) error {
	err := os.MkdirAll(recordingDir, 0o755)
	if err != nil {
		return err
	}

	// Sanitize stream name for use in filename (replace path separators with underscores)
	sanitized := strings.ReplaceAll(streamName, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, "\\", "_")

	// Use stream name for readable index filename.
	// Important: Index stores absolute segment file paths. Moving or deleting segment files
	// will break seeking. Keep segments in their original directory.
	k.path = filepath.Join(recordingDir, fmt.Sprintf("index-%s.jsonl", sanitized))
	f, err := os.OpenFile(k.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	k.f = f
	return nil
}

func (k *KeyframeIndex) Append(entry KeyframeIndexEntry) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.f == nil {
		return fmt.Errorf("index not initialized")
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	data = append(data, '\n')
	_, err = k.f.Write(data)
	if err != nil {
		return err
	}

	// Flush to disk after each entry to ensure durability
	return k.f.Sync()
}

func (k *KeyframeIndex) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.f != nil {
		err := k.f.Close()
		k.f = nil
		return err
	}
	return nil
}
