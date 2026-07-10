package playback

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"

	mcodecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mp4/codecs"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/recordstore"
	"github.com/gin-gonic/gin"
)


// codecDetectHEVC opens the first segment and reads its init section to determine
// whether the primary video track uses HEVC. Returns false on any error (safe default).
func codecDetectHEVC(segments []*recordstore.Segment) bool {
	if len(segments) == 0 {
		return false
	}
	f, err := os.Open(segments[0].Fpath)
	if err != nil {
		return false
	}
	defer f.Close()

	init, _, err := segmentFMP4ReadHeader(f)
	if err != nil {
		return false
	}
	for _, track := range init.Tracks {
		switch track.Codec.(type) {
		case *mcodecs.H265:
			return true
		}
	}
	return false
}

// buildFFmpegArgs builds hardware (rkmpp) and software (libx264) argument slices
// explicitly — no magic-index replacement.
//
// The internal fMP4 pipe comes from seekAndMux via pipeR.
// Output is written to a disk file (outputPath) as a progressive MP4 suitable
// for http.ServeContent (movflags +faststart, not frag_keyframe+empty_moov).
func buildFFmpegArgs(sourceIsHEVC bool, outputPath string) (hwArgs []string, swArgs []string) {
	// Common prefix (before -i) when source is HEVC.
	hevcDecoder := []string{"-c:v", "hevc_rkmpp"}
	swHevcDecoder := []string{"-c:v", "hevc"}
	noDec := []string{}

	// Input / stream-select / output flags shared by both paths.
	commonMiddle := []string{
		"-i", "pipe:0",
		"-map", "0:v:0",
		"-map", "0:a:0?",
	}
	// Video encode: hardware path.
	hwEncode := []string{
		"-c:v", "h264_rkmpp",
		"-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
		"-g", "50",
	}
	// Video encode: software path.
	swEncode := []string{
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-crf", "23",
		"-g", "50",
	}
	audioArgs := []string{
		"-c:a", "copy",
	}
	outputArgs := []string{
		"-movflags", "+faststart",
		"-f", "mp4",
		outputPath,
	}

	banner := []string{"-hide_banner", "-loglevel", "warning"}

	if sourceIsHEVC {
		hwArgs = append(banner, hevcDecoder...)
	} else {
		hwArgs = append(banner, noDec...)
	}
	hwArgs = append(hwArgs, commonMiddle...)
	hwArgs = append(hwArgs, hwEncode...)
	hwArgs = append(hwArgs, audioArgs...)
	hwArgs = append(hwArgs, outputArgs...)

	if sourceIsHEVC {
		swArgs = append(banner, swHevcDecoder...)
	} else {
		swArgs = append(banner, noDec...)
	}
	swArgs = append(swArgs, commonMiddle...)
	swArgs = append(swArgs, swEncode...)
	swArgs = append(swArgs, audioArgs...)
	swArgs = append(swArgs, outputArgs...)

	return hwArgs, swArgs
}

// seekableGenerate generates a complete non-transcoded MP4 into tmpPath.
func seekableGenerate(
	recordFormat conf.RecordFormat,
	segments []*recordstore.Segment,
	start time.Time,
	duration time.Duration,
	tmpPath string,
) error {
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %w", err)
	}
	defer f.Close()

	m := &muxerMP4{w: f}
	if err := seekAndMux(recordFormat, segments, start, duration, m); err != nil {
		return err
	}
	return nil
}

// seekableGenerateTranscode generates an H.264 MP4 via FFmpeg, writing output to tmpPath.
func (s *Server) seekableGenerateTranscode(
	ctx *gin.Context,
	recordFormat conf.RecordFormat,
	segments []*recordstore.Segment,
	start time.Time,
	duration time.Duration,
	tmpPath string,
) error {
	sourceIsHEVC := codecDetectHEVC(segments)
	hwArgs, swArgs := buildFFmpegArgs(sourceIsHEVC, tmpPath)

	const hostWrapper = "/usr/local/bin/mtx-host.sh"
	const hostFFmpeg = "/usr/bin/ffmpeg"

	// Create the fMP4 pipe for the muxer → FFmpeg input.
	pipeR, pipeW := io.Pipe()

	// Build the FFmpeg command.
	var cmd *exec.Cmd
	if _, statErr := os.Stat(hostWrapper); statErr == nil {
		s.Log(logger.Info, "transcode seekable: using hardware path via %s", hostWrapper)
		cmd = exec.CommandContext(ctx.Request.Context(), hostWrapper,
			append([]string{hostFFmpeg}, hwArgs...)...)
	} else {
		s.Log(logger.Info, "transcode seekable: %s not found, using software ffmpeg", hostWrapper)
		cmd = exec.CommandContext(ctx.Request.Context(), "ffmpeg", swArgs...)
	}

	cmd.Stdin = pipeR
	// FFmpeg writes directly to the output file (not stdout).
	cmd.Stdout = nil
	cmd.Stderr = &logWriter{s: s}

	if err := cmd.Start(); err != nil {
		pipeR.CloseWithError(err)
		pipeW.CloseWithError(err)
		return fmt.Errorf("ffmpeg start failed: %w", err)
	}

	// Run seekAndMux in a goroutine; FFmpeg reads from the pipe.
	muxErrCh := make(chan error, 1)
	go func() {
		m := &muxerFMP4{w: pipeW}
		err := seekAndMux(recordFormat, segments, start, duration, m)
		pipeW.CloseWithError(err)
		muxErrCh <- err
	}()

	ffmpegErr := cmd.Wait()
	muxResult := <-muxErrCh

	pipeR.Close()

	if ffmpegErr != nil {
		s.Log(logger.Error, "ffmpeg exited with error during seekable generation: %v", ffmpegErr)
		return fmt.Errorf("ffmpeg error: %w", ffmpegErr)
	}
	if muxResult != nil {
		s.Log(logger.Error, "muxer error during seekable transcode: %v", muxResult)
		return fmt.Errorf("mux error: %w", muxResult)
	}
	return nil
}

// onGetSeekable handles GET/HEAD /get?...&delivery=seekable.
//
// Flow:
//  1. Authenticate + validate (already done by caller).
//  2. Compute a deterministic cache key.
//  3. Call SeekableCache.Get to obtain (or generate) the final MP4 path.
//  4. Serve the file with http.ServeContent which handles byte-range, HEAD, etc.
func (s *Server) onGetSeekable(
	ctx *gin.Context,
	pathName string,
	start time.Time,
	duration time.Duration,
	format string,
	transcode string,
	pathConf *conf.Path,
	segments []*recordstore.Segment,
) {
	key := seekableCacheKey(pathName, start, duration, format, transcode, segments)

	var finalPath string
	var cacheErr error

	if transcode == "h264" {
		finalPath, cacheErr = s.seekableCache.Get(key, func(tmpPath string) error {
			return s.seekableGenerateTranscode(ctx, pathConf.RecordFormat, segments, start, duration, tmpPath)
		})
	} else {
		finalPath, cacheErr = s.seekableCache.Get(key, func(tmpPath string) error {
			return seekableGenerate(pathConf.RecordFormat, segments, start, duration, tmpPath)
		})
	}

	if cacheErr != nil {
		s.writeError(ctx, http.StatusInternalServerError,
			fmt.Errorf("seekable generation failed"))
		return
	}

	f, err := os.Open(finalPath)
	if err != nil {
		s.writeError(ctx, http.StatusInternalServerError,
			fmt.Errorf("cannot open cached file"))
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		s.writeError(ctx, http.StatusInternalServerError,
			fmt.Errorf("cannot stat cached file"))
		return
	}

	// Set security and cache headers.
	ctx.Header("X-Content-Type-Options", "nosniff")
	ctx.Header("Cache-Control", "private, no-store")
	ctx.Header("ETag", `"`+key+`"`)

	// http.ServeContent sets:
	//   Content-Type (detected or from name extension)
	//   Accept-Ranges: bytes
	//   Content-Length
	//   Content-Range (for partial content)
	//   Last-Modified
	//
	// We override Content-Type to ensure it is always video/mp4.
	ctx.Header("Content-Type", "video/mp4")

	http.ServeContent(ctx.Writer, ctx.Request, "recording.mp4", fi.ModTime(), f)
}
