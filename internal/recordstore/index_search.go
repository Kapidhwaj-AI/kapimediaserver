package recordstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bluenviron/mediamtx/internal/conf"
)

type KeyframeIndexEntry struct {
	WallTime   time.Time `json:"wall_time"`
	Segment    string    `json:"segment"`
	MonoPTS    int64     `json:"mono_pts"`
	IsGapStart bool      `json:"is_gap_start"`
}

// FindSegmentsViaIndex reads index.jsonl to find the exact sequence of segments and the MonoPTS start offset.
func FindSegmentsViaIndex(
	pathConf *conf.Path,
	pathName string,
	start *time.Time,
	end *time.Time,
) ([]*Segment, int64, error) {
	recordPath := PathAddExtension(
		strings.ReplaceAll(pathConf.RecordPath, "%path", pathName),
		pathConf.RecordFormat,
	)
	recordPath, _ = filepath.Abs(recordPath)
	dir := filepath.Dir(recordPath)

	indexPath := filepath.Join(dir, "index.jsonl")
	f, err := os.Open(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNoSegmentsFound
		}
		return nil, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	
	// We'll collect unique segments
	type segInfo struct {
		Path  string
		Start time.Time
	}
	var allSegments []segInfo
	var lastSegPath string
	
	var bestEntry *KeyframeIndexEntry
	var lastEntry *KeyframeIndexEntry

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		
		var entry KeyframeIndexEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		if entry.Segment != "" && entry.Segment != lastSegPath {
			allSegments = append(allSegments, segInfo{
				Path:  entry.Segment,
				Start: entry.WallTime, // We use the first keyframe's wall time as the segment start
			})
			lastSegPath = entry.Segment
		}

		if start != nil {
			if entry.WallTime.After(*start) {
				if bestEntry == nil {
					e := entry
					bestEntry = &e
				}
			} else {
				e := entry
				lastEntry = &e
			}
		}
	}

	if start != nil {
		if bestEntry == nil {
			if lastEntry != nil {
				bestEntry = lastEntry
			} else {
				return nil, 0, ErrNoSegmentsFound
			}
		} else if lastEntry != nil {
			// We want the keyframe exactly before or at start
			bestEntry = lastEntry
		}

		if bestEntry.Segment == "" {
			return nil, 0, errors.New("index entry missing segment path")
		}
	}

	if len(allSegments) == 0 {
		return nil, 0, ErrNoSegmentsFound
	}

	var segments []*Segment
	for _, s := range allSegments {
		if end != nil && s.Start.After(*end) {
			break
		}
		segments = append(segments, &Segment{
			Fpath: s.Path,
			Start: s.Start,
		})
	}

	if start != nil {
		// filter segments before start
		found := false
		for i := 0; i < len(segments)-1; i++ {
			if segments[i].Fpath == bestEntry.Segment {
				segments = segments[i:]
				found = true
				break
			}
		}
		if !found {
			if segments[len(segments)-1].Fpath == bestEntry.Segment {
				segments = segments[len(segments)-1:]
			} else {
				return nil, 0, ErrNoSegmentsFound
			}
		}
	}

	monoPTS := int64(0)
	if start != nil && bestEntry != nil {
		monoPTS = bestEntry.MonoPTS
	}

	return segments, monoPTS, nil
}
