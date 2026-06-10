package recorder

import (
	"time"

	"github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts"
	tscodecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts/codecs"
)

type formatMPEGTSTrack struct {
	f     *formatMPEGTS
	codec tscodecs.Codec

	track            *mpegts.Track
	startInitialized bool
	startDTS         time.Duration
	startNTP         time.Time
}

func (t *formatMPEGTSTrack) initialize() {
	t.track = &mpegts.Track{
		Codec: t.codec,
	}
}

func (t *formatMPEGTSTrack) write(
	dts time.Duration,
	ntp time.Time,
	randomAccess bool,
	cb func(track *mpegts.Track) error,
) error {
	isVideo := t.track.Codec.IsVideo()

	if isVideo {
		t.f.hasVideo = true
	}

	// Get monotonic PTS in nanoseconds based on the frame ingest time
	ptsNs, _ := t.f.ri.monoClock.Stamp(ntp)
	dts = ptsNs

	if !t.startInitialized {
		t.startDTS = dts
		t.startNTP = ntp
		t.startInitialized = true
	}

	// MPEG-TS keyframe indexing disabled: playback doesn't support MPEG-TS format yet.
	// Once MPEG-TS playback is implemented, re-enable indexing here.
	// if (!t.f.hasVideo || isVideo) && randomAccess {
	// 	segmentName := ""
	// 	if t.f.currentSegment != nil {
	// 		segmentName = t.f.currentSegment.path
	// 	}
	// 	err := t.f.ri.keyframeIndex.Append(KeyframeIndexEntry{
	// 		WallTime:   ntp,
	// 		Segment:    segmentName,
	// 		MonoPTS:    int64(ptsNs),
	// 		IsGapStart: isGap,
	// 	})
	// 	if err != nil {
	// 		t.f.ri.Log(logger.Warn, "failed to write keyframe index: %v", err)
	// 	}
	// }

	switch {
	case t.f.currentSegment == nil:
		t.f.currentSegment = &formatMPEGTSSegment{
			pathFormat2:       t.f.ri.pathFormat2,
			flush:             t.f.bw.Flush,
			onSegmentCreate:   t.f.ri.onSegmentCreate,
			onSegmentComplete: t.f.ri.onSegmentComplete,
			startDTS:          dts,
			startNTP:          ntp,
			log:               t.f.ri,
		}
		t.f.currentSegment.initialize()
		t.f.dw.setTarget(t.f.currentSegment)

	case (!t.f.hasVideo || isVideo) &&
		randomAccess &&
		(dts-t.f.currentSegment.startDTS) >= t.f.ri.segmentDuration:
		t.f.currentSegment.lastDTS = dts
		err := t.f.currentSegment.close()
		if err != nil {
			return err
		}

		t.f.currentSegment = &formatMPEGTSSegment{
			pathFormat2:       t.f.ri.pathFormat2,
			flush:             t.f.bw.Flush,
			onSegmentCreate:   t.f.ri.onSegmentCreate,
			onSegmentComplete: t.f.ri.onSegmentComplete,
			startDTS:          dts,
			startNTP:          ntp,
			log:               t.f.ri,
		}
		t.f.currentSegment.initialize()
		t.f.dw.setTarget(t.f.currentSegment)

	case (dts - t.f.currentSegment.lastFlush) >= t.f.ri.partDuration:
		err := t.f.bw.Flush()
		if err != nil {
			return err
		}

		t.f.currentSegment.lastFlush = dts
	}

	t.f.currentSegment.lastDTS = dts

	return cb(t.track)
}
