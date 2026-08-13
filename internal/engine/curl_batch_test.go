package engine

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// batchPattern is the deterministic byte pattern the fake dn node serves:
// every position is unique so a concatenation error shows up as a mismatch.
func batchPattern(i int64) byte { return byte((i*131 + 7) % 256) }

// batchDN is the fake archive.org dn node: serves /dn/file.bin with proper
// 206 range responses and records every Range header it receives.
type batchDN struct {
	mu     sync.Mutex
	ranges []string
}

func (d *batchDN) handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/dn/file.bin" {
		http.NotFound(w, r)
		return
	}
	rng := r.Header.Get("Range")
	d.mu.Lock()
	d.ranges = append(d.ranges, rng)
	d.mu.Unlock()
	var start, end int64
	if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}
	if end >= testBatchSize {
		end = testBatchSize - 1
	}
	b := make([]byte, end-start+1)
	for i := range b {
		b[i] = batchPattern(start + int64(i))
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, testBatchSize))
	w.Header().Set("Content-Length", fmt.Sprint(len(b)))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(b)
}

func (d *batchDN) recorded() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.ranges))
	copy(out, d.ranges)
	return out
}

// batchRelay is the fake CORS relay: the resolve hop (no redirect follow)
// gets a 302 to the dn node; wrapped requests (the resolved dn URL as path)
// are forwarded to the dn node with the Range header intact.
type batchRelay struct {
	dnURL    string
	rawURL   string
	resolves int
	wrapped  int
	mu       sync.Mutex
	client   *http.Client
}

func (rl *batchRelay) handler(w http.ResponseWriter, r *http.Request) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if r.URL.Path == "/"+rl.rawURL {
		// resolve hop: the "archive.org URL" as path -> 302 to the dn node
		rl.resolves++
		w.Header().Set("Location", rl.dnURL+"/dn/file.bin")
		w.WriteHeader(http.StatusFound)
		return
	}
	if !strings.Contains(r.URL.Path, "/dn/file.bin") {
		http.NotFound(w, r)
		return
	}
	rl.wrapped++
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, rl.dnURL+"/dn/file.bin", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header.Set("Range", r.Header.Get("Range"))
	resp, err := rl.client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

const testBatchSize = 1 << 20 // 1 MiB

// TestCurlBatchSegmentedParallel proves the segmented + parallel path: one
// job's segments are all fetched through ONE curl -Z invocation (verified by
// the exact request counts — one resolve, one request per segment, no
// retries), each segment gets exactly its own byte range, and the segments
// concatenate into the verified final file.
func TestCurlBatchSegmentedParallel(t *testing.T) {
	cs := NewCurlStreamer()
	if !cs.SupportsParallel() {
		t.Skip("curl lacks --parallel support")
	}
	dn := &batchDN{}
	dnSrv := httptest.NewServer(http.HandlerFunc(dn.handler))
	defer dnSrv.Close()
	rawURL := "https://archive.org/download/test/item.bin"
	rl := &batchRelay{dnURL: dnSrv.URL, rawURL: rawURL, client: http.DefaultClient}
	rlSrv := httptest.NewServer(http.HandlerFunc(rl.handler))
	defer rlSrv.Close()

	dir := t.TempDir()
	dest := filepath.Join(dir, "file.bin")
	expected := make([]byte, testBatchSize)
	for i := range expected {
		expected[i] = batchPattern(int64(i))
	}
	sum := sha1.Sum(expected)

	cs.Set = NewRelaySet(rlSrv.URL)
	e := &Engine{Streamer: cs, Segments: 4, SegmentMin: 1, Verify: true, Quiet: true, MaxTries: 3}
	j := Job{Name: "file.bin", URL: rawURL, Dest: dest, Size: testBatchSize, SHA1: hex.EncodeToString(sum[:])}
	var st Stats
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := e.do(ctx, &st, j); err != nil {
		t.Fatalf("do: %v", err)
	}

	// final file: bytes match the pattern exactly (concatenation correct)
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if len(got) != testBatchSize {
		t.Fatalf("dest size = %d, want %d", len(got), testBatchSize)
	}
	for i, b := range got {
		if b != expected[i] {
			t.Fatalf("byte %d = %d, want %d", i, b, expected[i])
		}
	}

	// the dn node saw exactly the 4 segment ranges, each once (any retry or a
	// collapsed range would show up here)
	want := []string{
		"bytes=0-262143",
		"bytes=262144-524287",
		"bytes=524288-786431",
		"bytes=786432-1048575",
	}
	rl.mu.Lock()
	resolves, wrapped := rl.resolves, rl.wrapped
	rl.mu.Unlock()
	if resolves != 1 {
		t.Errorf("resolve hops = %d, want 1 (resolve must run once per job)", resolves)
	}
	if wrapped != 4 {
		t.Errorf("wrapped segment requests = %d, want 4 (one curl -Z, no retries)", wrapped)
	}
	gotRanges := dn.recorded()
	if len(gotRanges) != len(want) {
		t.Fatalf("dn ranges = %v, want %v", gotRanges, want)
	}
	// curl -Z fetches the segments concurrently, so the order in which the dn
	// node records Range headers is nondeterministic: compare as sorted sets
	// (the multiset is exact — one request per segment, no retries).
	sort.Strings(gotRanges)
	sort.Strings(want)
	for i := range want {
		if gotRanges[i] != want[i] {
			t.Fatalf("dn range[%d] = %q, want %q", i, gotRanges[i], want[i])
		}
	}

	// no leftover .part / segment files
	if _, err := os.Stat(dest + ".part"); !os.IsNotExist(err) {
		t.Errorf(".part leftover: %v", err)
	}
	left, _ := filepath.Glob(dest + ".part.seg*")
	if len(left) != 0 {
		t.Errorf("segment leftovers: %v", left)
	}
}

// TestCurlBatchCancel verifies ctx cancellation kills the parallel curl and
// its backend requests promptly (no orphan subprocess).
func TestCurlBatchCancel(t *testing.T) {
	cs := NewCurlStreamer()
	if !cs.SupportsParallel() {
		t.Skip("curl lacks --parallel support")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// never respond: the request must die with the context
		<-r.Context().Done()
	}))
	defer slow.Close()
	rl := &batchRelay{dnURL: slow.URL, rawURL: "https://archive.org/download/test/item.bin", client: http.DefaultClient}
	rlSrv := httptest.NewServer(http.HandlerFunc(rl.handler))
	defer rlSrv.Close()
	cs.Set = NewRelaySet(rlSrv.URL)

	dir := t.TempDir()
	seg := SegSpec{Dest: filepath.Join(dir, "seg0"), Rng: "0-1048575"}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := cs.FetchBatch(ctx, "https://archive.org/download/test/item.bin", []SegSpec{seg})
	el := time.Since(start)
	if err == nil {
		t.Fatal("FetchBatch succeeded despite canceled context")
	}
	if el > 10*time.Second {
		t.Fatalf("FetchBatch took %v after cancellation, want prompt return", el)
	}
}

// TestCurlBatchResumePrunesCompleteSegments proves resume semantics on the
// parallel path: segments that already landed at their exact size are skipped
// (pruned) on the next attempt, only the missing ones are fetched, and the
// final concatenation is byte-identical.
func TestCurlBatchResumePrunesCompleteSegments(t *testing.T) {
	cs := NewCurlStreamer()
	if !cs.SupportsParallel() {
		t.Skip("curl lacks --parallel support")
	}
	dn := &batchDN{}
	dnSrv := httptest.NewServer(http.HandlerFunc(dn.handler))
	defer dnSrv.Close()
	rawURL := "https://archive.org/download/test/item.bin"
	rl := &batchRelay{dnURL: dnSrv.URL, rawURL: rawURL, client: http.DefaultClient}
	rlSrv := httptest.NewServer(http.HandlerFunc(rl.handler))
	defer rlSrv.Close()

	dir := t.TempDir()
	dest := filepath.Join(dir, "file.bin")
	part := dest + ".part"
	// pre-plant complete segments 0 and 1 (a previous attempt that got
	// interrupted after two segments landed)
	for i, rng := range [][2]int64{{0, 262143}, {262144, 524287}} {
		b := make([]byte, rng[1]-rng[0]+1)
		for j := range b {
			b[j] = batchPattern(rng[0] + int64(j))
		}
		if err := os.WriteFile(fmt.Sprintf("%s.seg%d", part, i), b, 0o644); err != nil {
			t.Fatalf("plant seg%d: %v", i, err)
		}
	}

	cs.Set = NewRelaySet(rlSrv.URL)
	e := &Engine{Streamer: cs, Segments: 4, SegmentMin: 1, Verify: true, Quiet: true, MaxTries: 3}
	j := Job{Name: "file.bin", URL: rawURL, Dest: dest, Size: testBatchSize}
	var st Stats
	ctx := context.Background()
	if _, err := e.do(ctx, &st, j); err != nil {
		t.Fatalf("do: %v", err)
	}

	rl.mu.Lock()
	resolves, wrapped := rl.resolves, rl.wrapped
	rl.mu.Unlock()
	if resolves != 1 {
		t.Errorf("resolve hops = %d, want 1", resolves)
	}
	if wrapped != 2 {
		t.Errorf("wrapped segment requests = %d, want 2 (complete segs must be pruned)", wrapped)
	}
	wantRanges := []string{"bytes=524288-786431", "bytes=786432-1048575"}
	got := dn.recorded()
	sort.Strings(got)
	sort.Strings(wantRanges)
	if len(got) != len(wantRanges) {
		t.Fatalf("dn ranges = %v, want %v", got, wantRanges)
	}
	for i := range wantRanges {
		if got[i] != wantRanges[i] {
			t.Fatalf("dn range[%d] = %q, want %q", i, got[i], wantRanges[i])
		}
	}

	// final file is byte-identical to the full pattern
	gotBytes, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if len(gotBytes) != testBatchSize {
		t.Fatalf("dest size = %d, want %d", len(gotBytes), testBatchSize)
	}
	for i, b := range gotBytes {
		if want := batchPattern(int64(i)); b != want {
			t.Fatalf("byte %d = %d, want %d", i, b, want)
		}
	}
}

// TestCurlBatchSingleSegment sanity: one segment goes through the parallel
// path and lands with the exact range bytes.
func TestCurlBatchSingleSegment(t *testing.T) {
	cs := NewCurlStreamer()
	if !cs.SupportsParallel() {
		t.Skip("curl lacks --parallel support")
	}
	dn := &batchDN{}
	dnSrv := httptest.NewServer(http.HandlerFunc(dn.handler))
	defer dnSrv.Close()
	rawURL := "https://archive.org/download/test/item.bin"
	rl := &batchRelay{dnURL: dnSrv.URL, rawURL: rawURL, client: http.DefaultClient}
	rlSrv := httptest.NewServer(http.HandlerFunc(rl.handler))
	defer rlSrv.Close()
	cs.Set = NewRelaySet(rlSrv.URL)

	dir := t.TempDir()
	dest := filepath.Join(dir, "seg0")
	ctx := context.Background()
	if err := cs.FetchBatch(ctx, rawURL, []SegSpec{{Dest: dest, Rng: "100-199"}}); err != nil {
		t.Fatalf("FetchBatch: %v", err)
	}
	b, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(b) != 100 {
		t.Fatalf("size = %d, want 100", len(b))
	}
	for i, v := range b {
		if want := batchPattern(100 + int64(i)); v != want {
			t.Fatalf("byte %d = %d, want %d", i, v, want)
		}
	}
}
