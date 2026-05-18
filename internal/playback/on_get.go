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

	ww := &writerWrapper{ctx: ctx}
	var m muxer

	format := ctx.Query("format")
	switch format {
	case "", "fmp4":
		m = &muxerFMP4{w: ww}

	case "mp4":
		m = &muxerMP4{w: ww}

	default:
		s.writeError(ctx, http.StatusBadRequest, fmt.Errorf("invalid format: %s", format))
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


	// ?transcode=h264: pipe fMP4 through ffmpeg → H.264 MP4 on the fly
	if ctx.Query("transcode") == "h264" {
		// Create a pipe: the muxer writes fMP4 to pipeW; ffmpeg reads from pipeR.
		pipeR, pipeW := io.Pipe()

		// Point the muxer at the write end of the pipe.
		// We use a plain writerWrapper that targets the pipe instead of the
		// ResponseWriter so that headers are set separately below.
		pipeWW := &writerWrapper{ctx: ctx}
		pipeWW.written = true // suppress writerWrapper's header injection; we set them manually
		switch format {
		case "", "fmp4":
			m = &muxerFMP4{w: pipeW}
		case "mp4":
			m = &muxerMP4{w: pipeW}
		}

		// Build the ffmpeg command.
		// stdin  → fMP4 stream from the muxer
		// stdout → H.264 MP4 streamed directly to the client
		//
		// Hardware path (Rockchip RK3588):
		//   The container's own ffmpeg is typically a stock build without rkmpp.
		//   The host ffmpeg (/usr/bin/ffmpeg, compiled with --enable-rkmpp) is reached
		//   via /usr/local/bin/mtx-host.sh — a host-namespace wrapper that is bind-mounted
		//   from the host into the container at /usr/local/bin.
		//   If mtx-host.sh is not found we fall back to the container's ffmpeg (software).
		const hostWrapper = "/usr/local/bin/mtx-host.sh"
		const hostFFmpeg = "/usr/bin/ffmpeg"
		ffmpegArgs := []string{
			"-hide_banner", "-loglevel", "warning", // suppress banner; log only warnings+
			"-c:v", "hevc_rkmpp", // hardware H.265 decoder (must precede -i)
			"-i", "pipe:0", // read fMP4 from stdin
			"-map", "0:v:0", // select first video stream
			"-map", "0:a:0?", // select first audio stream (? = optional, handles video-only)
			"-c:v", "h264_rkmpp", // hardware H.264 encoder
			"-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k", // CBR for smooth HTTP streaming
			"-g", "50", // keyframe every 50 frames (~2 s at 25 fps) — aids seeking
			"-c:a", "copy", // stream-copy audio (already AAC in recorded fMP4, zero CPU)
			"-movflags", "frag_keyframe+empty_moov", // fragmented MP4 required for pipe/HTTP output
			"-f", "mp4",
			"pipe:1", // write to stdout → client
		}

		var cmd *exec.Cmd
		if _, statErr := os.Stat(hostWrapper); statErr == nil {
			// Host-namespace wrapper found: use host's rkmpp-enabled ffmpeg.
			s.Log(logger.Info, "transcode: using hardware path via %s", hostWrapper)
			cmd = exec.CommandContext(ctx.Request.Context(), hostWrapper,
				append([]string{hostFFmpeg}, ffmpegArgs...)...)
		} else {
			// Fallback: container's own ffmpeg (software codecs only).
			s.Log(logger.Info, "transcode: %s not found, falling back to software ffmpeg", hostWrapper)
			ffmpegArgs[5] = "hevc" // replace hevc_rkmpp decoder with software hevc
			// replace h264_rkmpp encoder with libx264
			for i, a := range ffmpegArgs {
				if a == "h264_rkmpp" {
					ffmpegArgs[i] = "libx264"
					// insert -preset veryfast -crf 23 after libx264
					tail := append([]string{"-preset", "veryfast", "-crf", "23"}, ffmpegArgs[i+1:]...)
					ffmpegArgs = append(ffmpegArgs[:i+1], tail...)
					break
				}
			}
			cmd = exec.CommandContext(ctx.Request.Context(), "ffmpeg", ffmpegArgs...)
		}

		cmd.Stdin = pipeR
		cmd.Stdout = ctx.Writer
		// Route ffmpeg stderr to the mediamtx log so errors are visible.
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
