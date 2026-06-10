package recorder

import (
	"sync"
	"time"
)

// MonotonicClock produces strictly monotonic timestamps for recording.
// It absorbs NTP jumps, queue jitter, and clock rate discontinuities.
// All output is relative to the stream epoch (first frame seen).
type MonotonicClock struct {
	mu           sync.Mutex
	gapThreshold time.Duration

	initialized bool
	monoStart   time.Time
	lastMono    time.Time
	lastEmitted time.Duration

	// OnGap is called when a silence > gapThreshold is detected.
	// wallStart: the end of the previous frame
	// wallEnd: the start of the new frame
	// monoPTS: the PTS offset where the gap begins
	OnGap func(wallStart, wallEnd time.Time, monoPTS time.Duration)
}

func (mc *MonotonicClock) Initialize(gapThreshold time.Duration) {
	if gapThreshold == 0 {
		gapThreshold = 2 * time.Second
	}
	mc.gapThreshold = gapThreshold
}

// Stamp returns the monotonic PTS for the current frame.
// wallNow should be the frame's wall clock time.
// Multiple tracks may call this concurrently with out-of-order timestamps.
// Returns (pts, isGap). If isGap is true, a gap was detected and OnGap was called.
func (mc *MonotonicClock) Stamp(wallNow time.Time) (pts time.Duration, isGap bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if !mc.initialized {
		mc.initialized = true
		mc.monoStart = wallNow
		mc.lastMono = wallNow
		mc.lastEmitted = 0
		return 0, false
	}

	elapsed := wallNow.Sub(mc.lastMono)

	// If this frame is from the past (multi-track concurrency), ignore the backward jump
	// but still emit a monotonic timestamp based on the frame's actual position
	if elapsed < 0 {
		// Frame arrived out-of-order: use zero elapsed, don't update lastMono
		elapsed = 0
		return mc.lastEmitted, false
	}

	// Detect gaps (silence > threshold)
	if elapsed > mc.gapThreshold {
		isGap = true
		if mc.OnGap != nil {
			mc.OnGap(mc.lastMono, wallNow, mc.lastEmitted)
		}
	}

	// Always advance the monotonic clock by actual elapsed time
	mc.lastEmitted += elapsed
	mc.lastMono = wallNow
	return mc.lastEmitted, isGap
}
