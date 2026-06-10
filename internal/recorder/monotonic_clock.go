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
// wallNow should be time.Now() called by the caller.
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

	if elapsed > mc.gapThreshold {
		isGap = true
		if mc.OnGap != nil {
			mc.OnGap(mc.lastMono, wallNow, mc.lastEmitted)
		}
		// Advance monotonic clock by the exact gap
		mc.lastEmitted += elapsed
		mc.lastMono = wallNow
		return mc.lastEmitted, true
	}

	if elapsed < 0 {
		elapsed = 0
	}

	mc.lastEmitted += elapsed
	mc.lastMono = wallNow
	return mc.lastEmitted, false
}
