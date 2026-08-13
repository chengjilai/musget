package engine

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestSegmentedDirectOffsetResume exercises the Go HTTP segmented path
// (direct-offset writes into the shared .part file): a first run is cut short
// mid-segment by a dying server, a second run must resume from the .meta
// sidecars and produce a byte-identical, checksum-verified file.
func TestSegmentedDirectOffsetResume(t *testing.T) {
	dir := t.TempDir()
	data := make([]byte, 20<<20)
	for i := range data {
		data[i] = byte(i*7 + 13)
	}
	sum := md5.Sum(data)
	md5hex := hex.EncodeToString(sum[:])

	var mu sync.Mutex
	trunc := true // first run: each ranged response dies after ~35%
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tc := trunc
		mu.Unlock()
		rng := r.Header.Get("Range")
		if rng == "" {
			http.ServeContent(w, r, "x.bin", time.Time{}, bytes.NewReader(data))
			return
		}
		var a, b int64
		if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &a, &b); err != nil {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if !tc {
			http.ServeContent(w, r, "x.bin", time.Time{}, bytes.NewReader(data))
			return
		}
		// serve 35% of the requested range, then drop the connection
		end := a + (b-a+1)*35/100
		if end > b {
			end = b
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, end, len(data)))
		w.Header().Set("Content-Length", fmt.Sprint(end-a+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[a : end+1])
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, _ := hj.Hijack()
			conn.Close()
		}
	}))
	defer srv.Close()

	dest := filepath.Join(dir, "out.bin")
	eng := &Engine{
		Jobs:       4,
		Segments:   4,
		SegmentMin: 1 << 20,
		Verify:     true,
		HTTP:       srv.Client(),
		MaxTries:   2,
	}
	j := Job{Name: "out.bin", URL: srv.URL, Dest: dest, Size: int64(len(data)), MD5: md5hex}

	st1 := eng.Run(context.Background(), []Job{j})
	if st1.FilesOK != 0 || st1.FilesBad != 1 {
		t.Fatalf("run1 should fail, got ok=%d bad=%d", st1.FilesOK, st1.FilesBad)
	}
	if _, err := os.Stat(dest + ".part"); err != nil {
		t.Fatalf(".part missing after failed run: %v", err)
	}
	if _, err := os.Stat(segMetaPath(dest+".part", 0)); err != nil {
		t.Fatalf(".meta sidecar missing after failed run: %v", err)
	}

	mu.Lock()
	trunc = false
	mu.Unlock()
	st2 := eng.Run(context.Background(), []Job{j})
	if st2.FilesOK != 1 || st2.FilesBad != 0 {
		t.Fatalf("run2 failed: %v", st2.Lines)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded file differs from source (%d vs %d bytes)", len(got), len(data))
	}
	// sidecars must be cleaned up after success
	if _, err := os.Stat(segMetaPath(dest+".part", 0)); err == nil {
		t.Fatal(".meta sidecar not removed after successful run")
	}
}

// TestSingleStreamResume guards the single-stream (non-segmented) path: a
// dying server mid-body, then resume via the .part file size.
func TestSingleStreamResume(t *testing.T) {
	dir := t.TempDir()
	data := make([]byte, 4<<20)
	for i := range data {
		data[i] = byte(i * 3)
	}
	sum := md5.Sum(data)
	md5hex := hex.EncodeToString(sum[:])

	var mu sync.Mutex
	trunc := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tc := trunc
		mu.Unlock()
		if tc {
			a, b := int64(0), int64(len(data)-1)
			if rng := r.Header.Get("Range"); rng != "" {
				fmt.Sscanf(rng, "bytes=%d-%d", &a, &b)
			}
			n := (b - a + 1) * 2 / 5 // serve 40% of the range then drop
			end := a + n - 1
			if end > b {
				end = b
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, end, len(data)))
			w.Header().Set("Content-Length", fmt.Sprint(end-a+1))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data[a : end+1])
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, _ := hj.Hijack()
				conn.Close()
			}
			return
		}
		http.ServeContent(w, r, "x.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	dest := filepath.Join(dir, "out.bin")
	eng := &Engine{
		Jobs:     1,
		Verify:   true,
		HTTP:     srv.Client(),
		MaxTries: 2,
	}
	j := Job{Name: "out.bin", URL: srv.URL, Dest: dest, Size: int64(len(data)), MD5: md5hex}

	if st := eng.Run(context.Background(), []Job{j}); st.FilesOK != 0 {
		t.Fatalf("run1 should fail")
	}
	mu.Lock()
	trunc = false
	mu.Unlock()
	st := eng.Run(context.Background(), []Job{j})
	if st.FilesOK != 1 {
		t.Fatalf("run2 failed: %v", st.Lines)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs from source")
	}
}

// entirely (always 200 with the full body): segments must still land in their
// own byte ranges.
func TestSegmentedDirectOffsetNoRange(t *testing.T) {
	dir := t.TempDir()
	data := make([]byte, 8<<20)
	for i := range data {
		data[i] = byte(i * 11)
	}
	sum := md5.Sum(data)
	md5hex := hex.EncodeToString(sum[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "x.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()

	dest := filepath.Join(dir, "out.bin")
	eng := &Engine{
		Jobs:       2,
		Segments:   2,
		SegmentMin: 1 << 20,
		Verify:     true,
		HTTP:       srv.Client(),
		MaxTries:   2,
	}
	j := Job{Name: "out.bin", URL: srv.URL, Dest: dest, Size: int64(len(data)), MD5: md5hex}
	st := eng.Run(context.Background(), []Job{j})
	if st.FilesOK != 1 {
		t.Fatalf("download failed: %v", st.Lines)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs from source")
	}
}
