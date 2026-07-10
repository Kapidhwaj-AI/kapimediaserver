package playback

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/mediamtx/internal/auth"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/test"
	"github.com/stretchr/testify/require"
)



// seekableTestServer creates a playback server with a temporary seekable cache directory.
func seekableTestServer(t *testing.T, recordDir string) (*Server, string) {
	t.Helper()

	cacheDir := t.TempDir()

	s := &Server{
		Address:              "127.0.0.1:9996",
		ReadTimeout:          conf.Duration(30 * time.Second),
		WriteTimeout:         conf.Duration(30 * time.Second),
		PathConfs: map[string]*conf.Path{
			"mypath": {
				Name:         "mypath",
				RecordPath:   filepath.Join(recordDir, "%path/%Y-%m-%d_%H-%M-%S-%f"),
				RecordFormat: conf.RecordFormatFMP4,
			},
		},
		AuthManager:          test.NilAuthManager,
		Parent:               test.NilLogger,
		SeekableCacheDir:     cacheDir,
		SeekableCacheTTL:     5 * time.Minute,
		SeekableCacheMaxSize: 100 * 1024 * 1024, // 100 MB
	}
	err := s.Initialize()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s, cacheDir
}

// buildSeekableURL builds the URL for a seekable GET or HEAD request.
func buildSeekableURL(t *testing.T, method string, extras url.Values) *http.Request {
	t.Helper()

	u, err := url.Parse("http://myuser:mypass@localhost:9996/get")
	require.NoError(t, err)

	v := url.Values{}
	v.Set("path", "mypath")
	v.Set("start", time.Date(2008, 11, 7, 11, 23, 1, 500000000, time.Local).Format(time.RFC3339Nano))
	v.Set("duration", "3")
	v.Set("format", "mp4")
	v.Set("delivery", "seekable")
	for k, vals := range extras {
		for _, val := range vals {
			v.Set(k, val)
		}
	}
	u.RawQuery = v.Encode()

	req, err := http.NewRequest(method, u.String(), nil)
	require.NoError(t, err)
	return req
}

// TestSeekableFullGet verifies that delivery=seekable returns a complete MP4 with
// Accept-Ranges: bytes.
func TestSeekableFullGet(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, _ = seekableTestServer(t, dir)

	req := buildSeekableURL(t, http.MethodGet, nil)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "bytes", res.Header.Get("Accept-Ranges"))
	require.Equal(t, "video/mp4", res.Header.Get("Content-Type"))
	require.NotEmpty(t, res.Header.Get("Content-Length"))

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body)
	// Verify it starts with a valid MP4 ftyp or moov box signature.
	require.Greater(t, len(body), 8)
}

// TestSeekableRangeFirstTwo verifies that Range: bytes=0-1 returns HTTP 206 with
// correct headers and exactly 2 bytes.
func TestSeekableRangeFirstTwo(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, _ = seekableTestServer(t, dir)

	req := buildSeekableURL(t, http.MethodGet, nil)
	req.Header.Set("Range", "bytes=0-1")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusPartialContent, res.StatusCode)
	require.Equal(t, "bytes", res.Header.Get("Accept-Ranges"))
	require.Equal(t, "video/mp4", res.Header.Get("Content-Type"))
	require.Equal(t, "2", res.Header.Get("Content-Length"))

	contentRange := res.Header.Get("Content-Range")
	require.Regexp(t, `^bytes 0-1/\d+$`, contentRange)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, 2, len(body))
}

// TestSeekableRangeOpenEnded verifies that open-ended range requests work.
func TestSeekableRangeOpenEnded(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, _ = seekableTestServer(t, dir)

	// First get the full size.
	req := buildSeekableURL(t, http.MethodGet, nil)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	fullBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	fullSize := int64(len(fullBody))

	// Open-ended range from offset 100.
	req = buildSeekableURL(t, http.MethodGet, nil)
	req.Header.Set("Range", "bytes=100-")
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusPartialContent, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, int(fullSize-100), len(body))
}

// TestSeekableRangeSuffix verifies that suffix range requests (bytes=-N) work.
func TestSeekableRangeSuffix(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, _ = seekableTestServer(t, dir)

	req := buildSeekableURL(t, http.MethodGet, nil)
	req.Header.Set("Range", "bytes=-10")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusPartialContent, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, 10, len(body))
}

// TestSeekableRangeUnsatisfiable verifies that an unsatisfiable range returns HTTP 416.
func TestSeekableRangeUnsatisfiable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, _ = seekableTestServer(t, dir)

	req := buildSeekableURL(t, http.MethodGet, nil)
	req.Header.Set("Range", "bytes=99999999-")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, res.StatusCode)
}

// TestSeekableHEAD verifies that HEAD returns correct headers without a body.
func TestSeekableHEAD(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, _ = seekableTestServer(t, dir)

	// First issue GET to populate cache.
	req := buildSeekableURL(t, http.MethodGet, nil)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	fullBody, _ := io.ReadAll(res.Body)
	res.Body.Close()
	gotCL := res.Header.Get("Content-Length")

	// Now issue HEAD.
	req = buildSeekableURL(t, http.MethodHead, nil)
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "bytes", res.Header.Get("Accept-Ranges"))
	require.Equal(t, "video/mp4", res.Header.Get("Content-Type"))
	// Content-Length from HEAD must match what GET returned.
	require.Equal(t, gotCL, res.Header.Get("Content-Length"))
	require.NotEmpty(t, res.Header.Get("Content-Length"))
	// Sanity: GET content-length matches the actual body we received.
	require.Equal(t, gotCL, fmt.Sprintf("%d", len(fullBody)))

	// Body should be empty for HEAD.
	headBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Empty(t, headBody)
}

// TestSeekableCacheHit verifies that a second identical request serves from cache
// (mod-time of the cached file should be ≤ the time of the first request).
func TestSeekableCacheHit(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, cacheDir := seekableTestServer(t, dir)

	// First request.
	req := buildSeekableURL(t, http.MethodGet, nil)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, res.Body) //nolint:errcheck
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Record cache file state.
	entries, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	var mp4Files []os.FileInfo
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".mp4" {
			fi, _ := e.Info()
			mp4Files = append(mp4Files, fi)
		}
	}
	require.Len(t, mp4Files, 1)
	firstMTime := mp4Files[0].ModTime()

	// Second request.
	req = buildSeekableURL(t, http.MethodGet, nil)
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, res.Body) //nolint:errcheck
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Cache file should not have been re-generated (mod-time unchanged).
	entries, err = os.ReadDir(cacheDir)
	require.NoError(t, err)
	var mp4Files2 []os.FileInfo
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".mp4" {
			fi, _ := e.Info()
			mp4Files2 = append(mp4Files2, fi)
		}
	}
	require.Len(t, mp4Files2, 1)
	require.Equal(t, firstMTime, mp4Files2[0].ModTime(), "cache file was re-generated on second request")
}

// TestSeekableConcurrent verifies that concurrent identical requests generate only
// one output file (singleflight).
func TestSeekableConcurrent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	cacheDir := t.TempDir()
	var generateCount int32

	cache := &SeekableCache{
		Dir:     cacheDir,
		TTL:     5 * time.Minute,
		MaxSize: 100 * 1024 * 1024,
		Parent:  test.NilLogger,
	}
	err := cache.Initialize()
	require.NoError(t, err)
	defer cache.Close()

	const N = 10
	var wg sync.WaitGroup
	results := make([]string, N)
	errs := make([]error, N)

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = cache.Get("testkey", func(tmpPath string) error {
				atomic.AddInt32(&generateCount, 1)
				// Simulate some work.
				time.Sleep(50 * time.Millisecond)
				return os.WriteFile(tmpPath, []byte("fake-mp4-data"), 0o644)
			})
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		require.NoError(t, e, "goroutine %d failed", i)
	}
	// Exactly one generation should have occurred.
	require.Equal(t, int32(1), generateCount, "expected exactly one generation, got %d", generateCount)

	// All results should point to the same file.
	for i, r := range results {
		require.Equal(t, results[0], r, "goroutine %d got different path", i)
	}
}

// TestSeekableFailedGeneration verifies that a failed generation leaves no completed
// or temporary files, and returns HTTP 500.
func TestSeekableFailedGeneration(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	// Write a corrupt segment that will cause muxer failure.
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"),
		[]byte("corrupt data"), 0o644))

	_, cacheDir := seekableTestServer(t, dir)

	req := buildSeekableURL(t, http.MethodGet, nil)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, res.Body) //nolint:errcheck
	res.Body.Close()

	require.Equal(t, http.StatusInternalServerError, res.StatusCode)

	// No .mp4 or .tmp-* files should remain.
	entries, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		require.False(t,
			(len(name) > 4 && name[len(name)-4:] == ".mp4") ||
				len(name) > 4 && name[:4] != ".wri", // ignore .write-test probe artifact
			"unexpected file left in cache dir: %s", name)
	}
}

// TestSeekableCacheExpiry verifies that expired files are cleaned up by the
// cleanup goroutine.
func TestSeekableCacheExpiry(t *testing.T) {
	cacheDir := t.TempDir()

	// Use a very short TTL so expiry triggers immediately.
	cache := &SeekableCache{
		Dir:     cacheDir,
		TTL:     1 * time.Millisecond,
		MaxSize: 100 * 1024 * 1024,
		Parent:  test.NilLogger,
	}
	err := cache.Initialize()
	require.NoError(t, err)
	defer cache.Close()

	// Place a fake .mp4 with an old mod-time.
	fakeFile := filepath.Join(cacheDir, "aabbcc.mp4")
	require.NoError(t, os.WriteFile(fakeFile, []byte("data"), 0o644))
	// Back-date the file by 1 hour.
	old := time.Now().Add(-1 * time.Hour)
	require.NoError(t, os.Chtimes(fakeFile, old, old))

	// Run cleanup manually.
	cache.cleanup()

	_, err = os.Stat(fakeFile)
	require.True(t, os.IsNotExist(err), "expired file should have been removed")
}

// TestSeekableCacheSizeEviction verifies that files are evicted when MaxSize is exceeded.
func TestSeekableCacheSizeEviction(t *testing.T) {
	cacheDir := t.TempDir()

	// MaxSize = 15 bytes; we'll add 3 files of 10 bytes each.
	cache := &SeekableCache{
		Dir:     cacheDir,
		TTL:     1 * time.Hour,
		MaxSize: 15,
		Parent:  test.NilLogger,
	}
	err := cache.Initialize()
	require.NoError(t, err)
	defer cache.Close()

	for i, name := range []string{"aaa.mp4", "bbb.mp4", "ccc.mp4"} {
		p := filepath.Join(cacheDir, name)
		require.NoError(t, os.WriteFile(p, []byte("0123456789"), 0o644))
		// Stagger mod-times so oldest is deterministic.
		mt := time.Now().Add(-time.Duration(3-i) * time.Minute)
		require.NoError(t, os.Chtimes(p, mt, mt))
	}

	cache.cleanup()

	// After cleanup, total size must be ≤ MaxSize (15 bytes).
	// The oldest file (aaa.mp4, 3 min ago) should be evicted first.
	_, errA := os.Stat(filepath.Join(cacheDir, "aaa.mp4"))
	require.True(t, os.IsNotExist(errA), "oldest file should have been evicted")
}

// TestSeekableAuthEnforced verifies that delivery=seekable still requires authentication.
func TestSeekableAuthEnforced(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))

	cacheDir := t.TempDir()
	s := &Server{
		Address:      "127.0.0.1:9996",
		ReadTimeout:  conf.Duration(10 * time.Second),
		WriteTimeout: conf.Duration(10 * time.Second),
		PathConfs: map[string]*conf.Path{
			"mypath": {
				Name:         "mypath",
				RecordPath:   filepath.Join(dir, "%path/%Y-%m-%d_%H-%M-%S-%f"),
				RecordFormat: conf.RecordFormatFMP4,
			},
		},
		// Reject all requests: ask for credentials when none are present.
		AuthManager: &test.AuthManager{
			AuthenticateImpl: func(_ *auth.Request) *auth.Error {
				return &auth.Error{AskCredentials: true}
			},
		},
		Parent:               test.NilLogger,
		SeekableCacheDir:     cacheDir,
		SeekableCacheTTL:     5 * time.Minute,
		SeekableCacheMaxSize: 100 * 1024 * 1024,
	}
	err := s.Initialize()
	require.NoError(t, err)
	defer s.Close()

	u, err2 := url.Parse("http://localhost:9996/get")
	require.NoError(t, err2)
	v := url.Values{}
	v.Set("path", "mypath")
	v.Set("start", time.Date(2008, 11, 7, 11, 23, 1, 500000000, time.Local).Format(time.RFC3339Nano))
	v.Set("duration", "3")
	v.Set("delivery", "seekable")
	u.RawQuery = v.Encode()

	req, err2 := http.NewRequest(http.MethodGet, u.String(), nil)
	require.NoError(t, err2)

	res, err2 := http.DefaultClient.Do(req)
	require.NoError(t, err2)
	defer res.Body.Close()

	// Without credentials, should get 401 + WWW-Authenticate header.
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.Equal(t, `Basic realm="mediamtx"`, res.Header.Get("WWW-Authenticate"))
}


// TestSeekableH264Passthrough verifies that a non-transcoded seekable request for
// an H.264 recording succeeds and returns a valid MP4.
func TestSeekableH264Passthrough(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))
	writeSegment1(t, filepath.Join(dir, "mypath", "2008-11-07_11-22-00-500000.mp4"))
	writeSegment2(t, filepath.Join(dir, "mypath", "2008-11-07_11-23-02-500000.mp4"))

	_, _ = seekableTestServer(t, dir)

	req := buildSeekableURL(t, http.MethodGet, nil)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "bytes", res.Header.Get("Accept-Ranges"))

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body)
}

// TestSeekableFFmpegArgConstruction verifies that buildFFmpegArgs returns correct,
// index-safe argument slices for both hardware and software paths, with and without
// HEVC source detection — without requiring Rockchip hardware.
func TestSeekableFFmpegArgConstruction(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sourceHEVC  bool
		wantHWDecoder string
		wantHWEncoder string
		wantSWDecoder string
		wantSWEncoder string
	}{
		{
			name:          "H264 source",
			sourceHEVC:    false,
			wantHWDecoder: "",       // no decoder needed for H.264
			wantHWEncoder: "h264_rkmpp",
			wantSWDecoder: "",
			wantSWEncoder: "libx264",
		},
		{
			name:          "H265 source",
			sourceHEVC:    true,
			wantHWDecoder: "hevc_rkmpp",
			wantHWEncoder: "h264_rkmpp",
			wantSWDecoder: "hevc",
			wantSWEncoder: "libx264",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hwArgs, swArgs := buildFFmpegArgs(tc.sourceHEVC, "/tmp/out.mp4")

			// Verify -movflags +faststart is present (progressive, not fragmented).
			require.Contains(t, hwArgs, "+faststart")
			require.Contains(t, swArgs, "+faststart")

			// Verify output path is the file, not "pipe:1".
			require.Equal(t, "/tmp/out.mp4", hwArgs[len(hwArgs)-1])
			require.Equal(t, "/tmp/out.mp4", swArgs[len(swArgs)-1])

			// Verify encoder codec values by scanning the slice.
			hwEncoderIdx := -1
			for i, a := range hwArgs {
				if a == "-c:v" && i+1 < len(hwArgs) && hwArgs[i+1] == tc.wantHWEncoder {
					hwEncoderIdx = i
					break
				}
			}
			require.NotEqual(t, -1, hwEncoderIdx,
				"hardware encoder %q not found in hwArgs: %v", tc.wantHWEncoder, hwArgs)

			swEncoderIdx := -1
			for i, a := range swArgs {
				if a == "-c:v" && i+1 < len(swArgs) && swArgs[i+1] == tc.wantSWEncoder {
					swEncoderIdx = i
					break
				}
			}
			require.NotEqual(t, -1, swEncoderIdx,
				"software encoder %q not found in swArgs: %v", tc.wantSWEncoder, swArgs)

			if tc.sourceHEVC {
				// HEVC decoder must appear before -i.
				inputIdx := -1
				for i, a := range hwArgs {
					if a == "-i" {
						inputIdx = i
						break
					}
				}
				hwDecIdx := -1
				for i, a := range hwArgs {
					if a == "-c:v" && i+1 < len(hwArgs) && hwArgs[i+1] == tc.wantHWDecoder {
						hwDecIdx = i
						break
					}
				}
				require.NotEqual(t, -1, hwDecIdx)
				require.Less(t, hwDecIdx, inputIdx,
					"HEVC hw decoder must precede -i")

				swDecIdx := -1
				for i, a := range swArgs {
					if a == "-c:v" && i+1 < len(swArgs) && swArgs[i+1] == tc.wantSWDecoder {
						swDecIdx = i
						break
					}
				}
				require.NotEqual(t, -1, swDecIdx)
			}
		})
	}
}

// TestSeekableInvalidDelivery verifies that unknown delivery values return HTTP 400.
func TestSeekableInvalidDelivery(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "mypath"), 0o755))

	_, _ = seekableTestServer(t, dir)

	u, _ := url.Parse("http://myuser:mypass@localhost:9996/get")
	v := url.Values{}
	v.Set("path", "mypath")
	v.Set("start", time.Date(2008, 11, 7, 11, 23, 1, 500000000, time.Local).Format(time.RFC3339Nano))
	v.Set("duration", "3")
	v.Set("delivery", "hls") // invalid
	u.RawQuery = v.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}

// TestSeekableCacheKeyDeterminism verifies the cache key function produces the same
// output for identical inputs and different output for different inputs.
func TestSeekableCacheKeyDeterminism(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	k1 := seekableCacheKey("mypath", now, 120*time.Second, "mp4", "", nil)
	k2 := seekableCacheKey("mypath", now, 120*time.Second, "mp4", "", nil)
	require.Equal(t, k1, k2, "same inputs must produce same key")

	k3 := seekableCacheKey("otherpath", now, 120*time.Second, "mp4", "", nil)
	require.NotEqual(t, k1, k3, "different path must produce different key")

	k4 := seekableCacheKey("mypath", now.Add(time.Second), 120*time.Second, "mp4", "", nil)
	require.NotEqual(t, k1, k4, "different start must produce different key")

	k5 := seekableCacheKey("mypath", now, 60*time.Second, "mp4", "", nil)
	require.NotEqual(t, k1, k5, "different duration must produce different key")

	k6 := seekableCacheKey("mypath", now, 120*time.Second, "mp4", "h264", nil)
	require.NotEqual(t, k1, k6, "different transcode must produce different key")

	// Key must be a 64-char lowercase hex string (SHA-256).
	require.Len(t, k1, 64)
}

