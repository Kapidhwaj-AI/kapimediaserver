package playback

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/bluenviron/mediamtx/internal/recordstore"
)

// seekableCacheKey computes a deterministic, filesystem-safe SHA-256 cache key.
//
// The key covers:
//   - Recording path name
//   - Start time (UTC, nanosecond precision)
//   - Duration
//   - Output format (mp4/fmp4)
//   - Transcode mode (e.g. "h264" or "")
//   - Segment fingerprints: file path + size + modification time for each source segment
//
// Including segment fingerprints means that if a recording segment grows or is replaced,
// the cache key changes and a fresh file is generated rather than reusing stale output.
func seekableCacheKey(
	pathName string,
	start time.Time,
	duration time.Duration,
	format string,
	transcode string,
	segments []*recordstore.Segment,
) string {
	h := sha256.New()

	fmt.Fprintf(h, "path=%s\n", pathName)
	fmt.Fprintf(h, "start=%s\n", start.UTC().Format(time.RFC3339Nano))
	fmt.Fprintf(h, "duration=%s\n", duration)
	fmt.Fprintf(h, "format=%s\n", format)
	fmt.Fprintf(h, "transcode=%s\n", transcode)

	for _, seg := range segments {
		fmt.Fprintf(h, "seg=%s\n", seg.Fpath)
		// Include size and mtime so a growing/replaced file produces a different key.
		if fi, err := os.Stat(seg.Fpath); err == nil {
			fmt.Fprintf(h, "seg-size=%d\n", fi.Size())
			fmt.Fprintf(h, "seg-mtime=%d\n", fi.ModTime().UnixNano())
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}
