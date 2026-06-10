package recorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
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

func (k *KeyframeIndex) Initialize(segmentFormatPath string, streamID uuid.UUID) error {
	dir := filepath.Dir(segmentFormatPath)
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return err
	}

	// Use streamID to isolate index per recording session.
	// Important: Index stores absolute segment file paths. Moving or deleting segment files
	// will break seeking. Keep segments in their original directory.
	k.path = filepath.Join(dir, fmt.Sprintf("index-%s.jsonl", streamID))
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
