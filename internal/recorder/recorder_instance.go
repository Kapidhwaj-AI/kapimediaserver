package recorder

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/recordstore"
	"github.com/bluenviron/mediamtx/internal/stream"
)

type recorderInstance struct {
	pathFormat        string
	format            conf.RecordFormat
	partDuration      time.Duration
	maxPartSize       conf.StringSize
	segmentDuration   time.Duration
	pathName          string
	stream            *stream.Stream
	onSegmentCreate   OnSegmentCreateFunc
	onSegmentComplete OnSegmentCompleteFunc
	parent            logger.Writer

	streamID    uuid.UUID
	pathFormat2 string
	format2     format
	skip        bool
	reader      *stream.Reader

	terminate chan struct{}
	done      chan struct{}

	monoClock     MonotonicClock
	keyframeIndex KeyframeIndex
}

// Log implements logger.Writer.
func (ri *recorderInstance) Log(level logger.Level, format string, args ...any) {
	ri.parent.Log(level, format, args...)
}

func (ri *recorderInstance) initialize() {
	ri.streamID = uuid.New()
	ri.pathFormat2 = ri.pathFormat
	ri.pathFormat2 = recordstore.PathAddExtension(
		strings.ReplaceAll(ri.pathFormat2, "%path", ri.pathName),
		ri.format,
	)
	ri.reader = &stream.Reader{
		SkipBytesSent: true,
		Parent:        ri,
	}

	ri.terminate = make(chan struct{})
	ri.done = make(chan struct{})

	// Initialize keyframe index in the static root of the recording directory.
	// pathFormat2 still contains date/time tokens (%Y, %m, etc.) so we MUST use
	// CommonPath() — which stops at the first token — to get the real directory.
	// This is the same logic used by FindSegmentsViaIndex in index_search.go,
	// so writer and reader always agree on where the index file lives.
	indexDir := recordstore.CommonPath(ri.pathFormat2)
	if err := ri.keyframeIndex.Initialize(indexDir, ri.pathName); err != nil {
		ri.Log(logger.Warn, "failed to initialize keyframe index: %v", err)
	}
	ri.monoClock.Initialize(2 * time.Second)
	ri.monoClock.OnGap = func(wallStart, wallEnd time.Time, monoPTS time.Duration) {
		ri.Log(logger.Info, "gap detected: %s to %s (duration: %s)", wallStart, wallEnd, wallEnd.Sub(wallStart))
		// We flag gap starts inside the keyframe index instead of a separate sidecar.
	}

	switch ri.format {
	case conf.RecordFormatMPEGTS:
		ri.format2 = &formatMPEGTS{
			ri: ri,
		}
		ok := ri.format2.initialize()
		ri.skip = !ok

	default:
		ri.format2 = &formatFMP4{
			ri: ri,
		}
		ok := ri.format2.initialize()
		ri.skip = !ok
	}

	if !ri.skip {
		ri.stream.AddReader(ri.reader)
	}

	go ri.run()
}

func (ri *recorderInstance) close() {
	close(ri.terminate)
	<-ri.done
}

func (ri *recorderInstance) run() {
	defer close(ri.done)

	if !ri.skip {
		select {
		case err := <-ri.reader.Error():
			ri.Log(logger.Error, err.Error())

		case <-ri.terminate:
		}

		ri.stream.RemoveReader(ri.reader)
	} else {
		<-ri.terminate
	}

	ri.keyframeIndex.Close()
	ri.format2.close()
}
