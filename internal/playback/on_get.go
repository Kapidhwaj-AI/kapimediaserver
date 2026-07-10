package playback

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/bluenviron/mediacommon/v2/pkg/formats/fmp4"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/recordstore"
	"github.com/gin-gonic/gin"
)

type writerWrapper struct {
	ctx     *gin.Context
	written bool
}

func (w *writerWrapper) Write(p []byte) (int, error) {
	if !w.written {
		w.written = true
		w.ctx.Header("Accept-Ranges", "none")
		w.ctx.Header("Content-Type", "video/mp4")
	}
	return w.ctx.Writer.Write(p)
}

// logWriter forwards ffmpeg's stderr lines into the mediamtx structured log.
type logWriter struct {
	s *Server
}

func (lw *logWriter) Write(p []byte) (int, error) {
	lw.s.Log(logger.Warn, "ffmpeg: %s", strings.TrimRight(string(p), "\r\n"))
	return len(p), nil
}

func parseDuration(raw string) (time.Duration, error) {
	// seconds
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		return time.Duration(secs * float64(time.Second)), nil
	}

	// deprecated, golang format
	return time.ParseDuration(raw)
}

func seekAndMux(
	recordFormat conf.RecordFormat,
	segments []*recordstore.Segment,
	start time.Time,
	duration time.Duration,
	m muxer,
) error {
	if recordFormat == conf.RecordFormatFMP4 {
		f, err := os.Open(segments[0].Fpath)
		if err != nil {
			return err
		}
		defer f.Close()

		firstInit, _, err := segmentFMP4ReadHeader(f)
		if err != nil {
			return err
		}

		m.writeInit(&fmp4.Init{
			Tracks: firstInit.Tracks,
		})

		firstMtxi := findMtxi(firstInit.UserData)
		startOffset := segments[0].Start.Sub(start) // this is negative
		dts := startOffset
		prevInit := firstInit

		segmentDuration, err := segmentFMP4MuxParts(f, dts, duration, firstInit.Tracks, m)
		if err != nil {
			return err
		}

		segmentEnd := segments[0].Start.Add(segmentDuration)

		for _, seg := range segments[1:] {
			f, err = os.Open(seg.Fpath)
			if err != nil {
				return err
			}
			defer f.Close()

			var init *fmp4.Init
			init, _, err = segmentFMP4ReadHeader(f)
			if err != nil {
				return err
			}

			if !segmentFMP4CanBeConcatenated(prevInit, segmentEnd, init, seg.Start) {
				break
			}

			if firstMtxi != nil {
				mtxi := findMtxi(init.UserData)
				dts = time.Duration(mtxi.DTS-firstMtxi.DTS) + startOffset
			} else { // legacy method
				dts = seg.Start.Sub(start) // this is positive
			}

			segmentDuration, err = segmentFMP4MuxParts(f, dts, duration, firstInit.Tracks, m)
			if err != nil {
				return err
			}

			segmentEnd = seg.Start.Add(segmentDuration)
			prevInit = init
		}

		err = m.flush()
		if err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("MPEG-TS format is not supported yet")
}

func (s *Server) onGet(ctx *gin.Context) {
	pathName := ctx.Query("path")

	if !s.doAuth(ctx, pathName) {
		return
	}

	start, err := time.Parse(time.RFC3339, ctx.Query("start"))
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid start: %w", err))
		return
	}

	duration, err := parseDuration(ctx.Query("duration"))
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid duration: %w", err))
		return
	}

	format := ctx.Query("format")
	switch format {
	case "", "fmp4", "mp4":
		// valid
	default:
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid format: %s", format))
		return
	}

	// Validate and route the delivery parameter.
	delivery := ctx.Query("delivery")
	switch delivery {
	case "", "seekable":
		// valid
	default:
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid delivery: %q (valid values: \"\", \"seekable\")", delivery))
		return
	}

	pathConf, err := s.safeFindPathConf(pathName)
	if err != nil {
		s.writeError(ctx, http.StatusBadRequest, err)
		return
	}

	end := start.Add(duration)
	segments, err := recordstore.FindSegments(pathConf, pathName, &start, &end)
	if err != nil {
		if errors.Is(err, recordstore.ErrNoSegmentsFound) {
			s.writeError(ctx, http.StatusNotFound, err)
		} else {
			s.writeError(ctx, http.StatusBadRequest, err)
		}
		return
	}

	// -----------------------------------------------------------------------
	// delivery=seekable: generate a complete cached MP4 and serve with ranges.
	// -----------------------------------------------------------------------
	if delivery == "seekable" {
		if s.seekableCache == nil {
			s.writeError(ctx, http.StatusBadRequest,
				fmt.Errorf("seekable delivery is not configured (playbackSeekableCacheDir is empty)"))
			return
		}
		if duration <= 0 {
			s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("duration must be greater than zero"))
			return
		}

		transcode := ctx.Query("transcode")
		if transcode != "" && transcode != "h264" {
			s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid transcode value: %q", transcode))
			return
		}

		s.onGetSeekable(ctx, pathName, start, duration, format, transcode, pathConf, segments)
		return
	}

	// -----------------------------------------------------------------------
	// ?transcode=h264: pipe fMP4 through ffmpeg → H.264 MP4 streamed to client.
	// -----------------------------------------------------------------------
	if ctx.Query("transcode") == "h264" {
		pipeR, pipeW := io.Pipe()

		var m muxer
		switch format {
		case "", "fmp4":
			m = &muxerFMP4{w: pipeW}
		case "mp4":
			m = &muxerMP4{w: pipeW}
		}

		const hostWrapper = "/usr/local/bin/mtx-host.sh"
		const hostFFmpeg = "/usr/bin/ffmpeg"

		// Detect source codec so we only apply the HEVC decoder when needed.
		sourceIsHEVC := codecDetectHEVC(segments)

		// Build hardware and software argument slices explicitly.
		// The output is fragmented MP4 piped to the client (streaming, not seekable).
		streamOutput := []string{
			"-movflags", "frag_keyframe+empty_moov",
			"-f", "mp4",
			"pipe:1",
		}

		banner := []string{"-hide_banner", "-loglevel", "warning"}
		input := []string{"-i", "pipe:0"}
		streamSel := []string{"-map", "0:v:0", "-map", "0:a:0?"}
		audioArgs := []string{"-c:a", "copy"}

		var hwArgs []string
		if sourceIsHEVC {
			hwArgs = append(banner, "-c:v", "hevc_rkmpp")
		} else {
			hwArgs = append([]string{}, banner...)
		}
		hwArgs = append(hwArgs, input...)
		hwArgs = append(hwArgs, streamSel...)
		hwArgs = append(hwArgs, "-c:v", "h264_rkmpp",
			"-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
			"-g", "50")
		hwArgs = append(hwArgs, audioArgs...)
		hwArgs = append(hwArgs, streamOutput...)

		var swArgs []string
		if sourceIsHEVC {
			swArgs = append(banner, "-c:v", "hevc")
		} else {
			swArgs = append([]string{}, banner...)
		}
		swArgs = append(swArgs, input...)
		swArgs = append(swArgs, streamSel...)
		swArgs = append(swArgs, "-c:v", "libx264",
			"-preset", "veryfast", "-crf", "23", "-g", "50")
		swArgs = append(swArgs, audioArgs...)
		swArgs = append(swArgs, streamOutput...)

		var cmd *exec.Cmd
		if _, statErr := os.Stat(hostWrapper); statErr == nil {
			s.Log(logger.Info, "transcode: using hardware path via %s", hostWrapper)
			cmd = exec.CommandContext(ctx.Request.Context(), hostWrapper,
				append([]string{hostFFmpeg}, hwArgs...)...)
		} else {
			s.Log(logger.Info, "transcode: %s not found, falling back to software ffmpeg", hostWrapper)
			cmd = exec.CommandContext(ctx.Request.Context(), "ffmpeg", swArgs...)
		}

		cmd.Stdin = pipeR
		cmd.Stdout = ctx.Writer
		cmd.Stderr = &logWriter{s: s}

		// Set response headers before the first byte is written.
		ctx.Header("Accept-Ranges", "none")
		ctx.Header("Content-Type", "video/mp4")

		if startErr := cmd.Start(); startErr != nil {
			pipeR.CloseWithError(startErr)
			pipeW.CloseWithError(startErr)
			s.writeError(ctx, http.StatusInternalServerError,
				fmt.Errorf("ffmpeg start failed: %w", startErr))
			return
		}

		// Run seekAndMux in a goroutine so we can Wait() on ffmpeg in this goroutine.
		muxErr := make(chan error, 1)
		go func() {
			err := seekAndMux(pathConf.RecordFormat, segments, start, duration, m)
			// Always close the write end so ffmpeg sees EOF.
			pipeW.CloseWithError(err)
			muxErr <- err
		}()

		// Wait for ffmpeg to finish (it will exit when its stdin is closed).
		ffmpegErr := cmd.Wait()

		// Drain the mux result so the goroutine doesn't leak.
		muxResult := <-muxErr

		pipeR.Close()

		// If the client disconnected, both errors are expected — ignore them.
		var neterr *net.OpError
		if errors.As(muxResult, &neterr) {
			return
		}

		if ffmpegErr != nil {
			s.Log(logger.Error, "ffmpeg exited with error: %v", ffmpegErr)
		}
		if muxResult != nil {
			s.Log(logger.Error, "muxer error during transcode: %v", muxResult)
		}
		return
	}

	// ---------------------------------------------------------------------------
	// Default path: write fMP4/MP4 directly to the ResponseWriter (unchanged)
	// ---------------------------------------------------------------------------

	ww := &writerWrapper{ctx: ctx}
	var m muxer
	switch format {
	case "", "fmp4":
		m = &muxerFMP4{w: ww}
	case "mp4":
		m = &muxerMP4{w: ww}
	}

	err = seekAndMux(pathConf.RecordFormat, segments, start, duration, m)
	if err != nil {
		// user aborted the download
		var neterr *net.OpError
		if errors.As(err, &neterr) {
			return
		}

		// nothing has been written yet; send back JSON
		if !ww.written {
			if errors.Is(err, recordstore.ErrNoSegmentsFound) {
				s.writeError(ctx, http.StatusNotFound, err)
			} else {
				s.writeError(ctx, http.StatusBadRequest, err)
			}
			return
		}

		// something has already been written: abort and write logs only
		s.Log(logger.Error, err.Error())
		return
	}
}
