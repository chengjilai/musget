package engine

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSegmentsFor(t *testing.T) {
	cases := []struct {
		size int64
		want int
	}{
		{1 << 20, 0},   // 1MB: single
		{7 << 20, 0},   // 7MB: single
		{8 << 20, 2},   // 8MB: 2
		{39 << 20, 2},  // 39MB: 2
		{40 << 20, 4},  // 40MB: 4
		{119 << 20, 4}, // 119MB: 4
		{120 << 20, 6}, // 120MB: 6
		{300 << 20, 6}, // 300MB: 6
	}
	for _, c := range cases {
		if got := segmentsFor(c.size); got != c.want {
			t.Errorf("segmentsFor(%d) = %d, want %d", c.size, got, c.want)
		}
	}
}

func TestBackoff(t *testing.T) {
	if backoff(1) != time.Second || backoff(2) != 2*time.Second || backoff(3) != 4*time.Second {
		t.Fatalf("backoff wrong: %v %v %v", backoff(1), backoff(2), backoff(3))
	}
	if backoff(10) != 30*time.Second {
		t.Fatalf("backoff cap wrong: %v", backoff(10))
	}
}

func TestRelayStalled(t *testing.T) {
	if !RelayStalled("curl: (28) Operation too slow. Less than 1024 bytes/sec transferred the last 15 seconds") {
		t.Fatal("expected stall detection")
	}
	if RelayStalled("curl: (28) Operation timed out after 10001 milliseconds") {
		t.Fatal("timeout should not count as stall")
	}
	if RelayStalled("") {
		t.Fatal("empty stderr should not count as stall")
	}
	if !RelayRateLimited("curl: (22) The requested URL returned error: 429") {
		t.Fatal("expected 429 rate-limit detection")
	}
	if RelayRateLimited("curl: (28) Operation too slow.") {
		t.Fatal("stall should not count as rate-limit")
	}
}

// fakeServer serves N-byte bodies with Content-Length so the engine can
// size-check downloads.
func fakeServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var size int64
		if s := r.URL.Query().Get("size"); s != "" {
			fmt.Sscanf(s, "%d", &size)
		}
		if size <= 0 {
			size = 1 << 20
		}
		w.Header().Set("Content-Length", fmt.Sprint(size))
		if r.Header.Get("Range") != "" {
			var start, end int64
			fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
			if end <= 0 {
				end = size - 1
			}
			if start >= size {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= size {
				end = size - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
			w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(make([]byte, end-start+1))
			return
		}
		w.Write(make([]byte, size))
	}))
}

// TestAdaptivePool runs mixed-size jobs through the adaptive pool and checks
// every job lands, files are the right size, and there is no deadlock.
func TestAdaptivePool(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()

	dir := t.TempDir()
	sizes := []int64{64 << 20, 3 << 20, 16 << 20, 1 << 20, 9 << 20, 2 << 20}
	var jobs []Job
	for i, sz := range sizes {
		jobs = append(jobs, Job{
			Name: fmt.Sprintf("f%d", i),
			URL:  fmt.Sprintf("%s/?size=%d", srv.URL, sz),
			Dest: filepath.Join(dir, fmt.Sprintf("f%d.bin", i)),
			Size: sz,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := &Engine{
		Jobs:     4,
		Adaptive: true,
		MinJobs:  2,
		MaxJobs:  12,
		Verify:   false,
		Quiet:    true,
		HTTP:     srv.Client(),
		MaxTries: 3,
	}
	st := e.Run(ctx, jobs)
	if st.FilesOK != len(jobs) || st.FilesBad != 0 {
		t.Fatalf("files ok=%d bad=%d want ok=%d", st.FilesOK, st.FilesBad, len(jobs))
	}
	for i, sz := range sizes {
		fi, err := os.Stat(filepath.Join(dir, fmt.Sprintf("f%d.bin", i)))
		if err != nil {
			t.Fatalf("missing file f%d: %v", i, err)
		}
		if fi.Size() != sz {
			t.Fatalf("f%d size = %d, want %d", i, fi.Size(), sz)
		}
	}
	if got := st.Bytes.Load(); got != sum64(sizes) {
		t.Fatalf("bytes = %d, want %d", got, sum64(sizes))
	}
}

// TestAdaptivePoolShrink verifies a failing relay (server that returns 500
// then recovers) triggers pool shrink behavior without deadlock, and partial
// bytes are counted for the failed job.
func TestAdaptivePoolShrink(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var gotHit atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHit.Add(1)
		if fail.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "1048576")
		w.Write(make([]byte, 1048576))
	}))
	defer srv.Close()

	dir := t.TempDir()
	var jobs []Job
	for i := 0; i < 6; i++ {
		jobs = append(jobs, Job{Name: fmt.Sprintf("g%d", i), URL: srv.URL, Dest: filepath.Join(dir, fmt.Sprintf("g%d", i)), Size: 1 << 20})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := &Engine{
		Jobs:     4,
		Adaptive: true,
		MinJobs:  2,
		MaxJobs:  8,
		Verify:   false,
		Quiet:    true,
		HTTP:     srv.Client(),
		MaxTries: 2, // fail fast on the broken server
	}
	st := e.Run(ctx, jobs)
	if st.FilesOK+st.FilesBad != len(jobs) {
		t.Fatalf("jobs accounted ok=%d bad=%d want %d", st.FilesOK, st.FilesBad, len(jobs))
	}
	if gotHit.Load() == 0 {
		t.Fatal("server never hit")
	}
	// engine must not hang even when a file fails permanently
	if st.FilesBad == 0 {
		t.Fatalf("expected failures against 500 server, got ok=%d", st.FilesOK)
	}
}

// TestOrderingDesc verifies Run sorts jobs largest-first by dispatching order
// (first job dispatched must be the largest).
func TestOrderingDesc(t *testing.T) {
	var order []int64
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var size int64
		fmt.Sscanf(r.URL.Query().Get("size"), "%d", &size)
		mu.Lock()
		order = append(order, size)
		mu.Unlock()
		w.Header().Set("Content-Length", fmt.Sprint(size))
		w.Write(make([]byte, size))
	}))
	defer srv.Close()

	dir := t.TempDir()
	sizes := []int64{5 << 20, 1 << 20, 40 << 20, 2 << 20}
	var jobs []Job
	for i, sz := range sizes {
		jobs = append(jobs, Job{Name: fmt.Sprintf("h%d", i), URL: fmt.Sprintf("%s/?size=%d", srv.URL, sz), Dest: filepath.Join(dir, fmt.Sprintf("h%d", i)), Size: sz})
	}
	e := &Engine{Jobs: 1, Verify: false, Quiet: true, HTTP: srv.Client(), MaxTries: 3}
	st := e.Run(context.Background(), jobs)
	if st.FilesBad != 0 {
		t.Fatalf("bad=%d", st.FilesBad)
	}
	mu.Lock()
	defer mu.Unlock()
	// with a single worker, dispatch order == processing order, so the
	// largest job must be requested first.
	if len(order) == 0 || order[0] != 40<<20 {
		t.Fatalf("largest job not dispatched first: %v", order)
	}
}

func sum64(s []int64) int64 {
	var t int64
	for _, v := range s {
		t += v
	}
	return t
}

func mkOutcome(ok bool, mb float64) jobOutcome {
	return jobOutcome{bytes: int64(mb * 1e6), dur: time.Second, ok: ok}
}

// TestPoolDecisionDec: a sustained collapse (majority failures + throughput
// drop) fires exactly one DEC, and never on sparse failures or rising
// throughput.
func TestPoolDecisionDec(t *testing.T) {
	win := make([]jobOutcome, aimdWindow)
	for i := range win {
		win[i] = mkOutcome(true, 1)
	}
	// 5 fails / 10 = 0.5 failure rate: NOT > 0.5 -> no DEC (dead relay in a
	// rotation produces exactly this alternating pattern).
	for i := 1; i < len(win); i += 2 {
		win[i] = mkOutcome(false, 0)
	}
	if _, act, _ := poolDecision(win, 20.0, 0); act == poolDec {
		t.Fatal("50%% failure rate must not DEC")
	}
	// 6 fails / 10 with throughput collapse -> exactly one DEC
	win[0] = mkOutcome(false, 0)
	tp, act, cd := poolDecision(win, 20.0, 0)
	if act != poolDec {
		t.Fatalf("action = %d, want poolDec (tp=%.2f)", act, tp)
	}
	if cd != aimdWindow {
		t.Fatalf("post-DEC cooldown = %d, want %d", cd, aimdWindow)
	}
	// DEC is gated on throughput actually dropping
	if _, act, _ := poolDecision(win, 0.4, 0); act == poolDec {
		t.Fatal("throughput not dropped -> no DEC")
	}
	// at most one DEC per window: cooldown still active
	if _, act, _ := poolDecision(win, 20.0, aimdWindow-1); act == poolDec {
		t.Fatal("DEC fired during cooldown")
	}
}

// TestPoolDecisionSparseFailsNoDec: a single isolated failure in an
// otherwise healthy window must never shrink the pool.
func TestPoolDecisionSparseFailsNoDec(t *testing.T) {
	win := make([]jobOutcome, aimdWindow)
	for i := range win {
		win[i] = mkOutcome(true, 1)
	}
	win[0] = mkOutcome(false, 0)
	if _, act, _ := poolDecision(win, 20.0, 0); act == poolDec {
		t.Fatal("a single failure must not DEC the pool")
	}
}

// TestPoolDecisionInc: clean rising windows grow the pool, short windows
// don't.
func TestPoolDecisionInc(t *testing.T) {
	win := make([]jobOutcome, aimdWindow)
	for i := range win {
		win[i] = mkOutcome(true, 2)
	}
	if _, act, _ := poolDecision(win, 1.0, 0); act != poolInc {
		t.Fatal("clean rising window must INC")
	}
	if _, act, _ := poolDecision(win[:aimdWindow/2], 1.0, 0); act == poolInc {
		t.Fatal("half-full window must not INC")
	}
}

// TestDecDesiredFloor: halving never goes below the floor and never raises
// desired.
func TestDecDesiredFloor(t *testing.T) {
	if decDesired(16, 8) != 8 {
		t.Fatal("16 halved to 8")
	}
	if decDesired(8, 8) != 8 {
		t.Fatal("at floor stays put")
	}
	if decDesired(7, 8) != 7 {
		t.Fatal("floor never raises desired")
	}
	if decDesired(16, 2) != 8 {
		t.Fatal("normal halving")
	}
}

// TestAdaptivePoolFlaky runs jobs against a server that permanently fails
// every even-indexed job (~50%% of the batch, the dead-relay-in-rotation
// pattern): the pool must not collapse and every job must be accounted for.
func TestAdaptivePoolFlaky(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id int64
		fmt.Sscanf(r.URL.Query().Get("id"), "%d", &id)
		if id%2 == 0 {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "1048576")
		w.Write(make([]byte, 1048576))
	}))
	defer srv.Close()

	dir := t.TempDir()
	var jobs []Job
	for i := 0; i < 24; i++ {
		jobs = append(jobs, Job{
			Name: fmt.Sprintf("flaky%d", i),
			URL:  fmt.Sprintf("%s/?id=%d", srv.URL, i),
			Dest: filepath.Join(dir, fmt.Sprintf("flaky%d", i)),
			Size: 1 << 20,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := &Engine{
		Jobs:     8,
		Adaptive: true,
		MinJobs:  2,
		MaxJobs:  16,
		Verify:   false,
		Quiet:    true,
		HTTP:     srv.Client(),
		MaxTries: 2,
	}
	st := e.Run(ctx, jobs)
	if st.FilesOK+st.FilesBad != len(jobs) {
		t.Fatalf("accounted ok=%d bad=%d want %d", st.FilesOK, st.FilesBad, len(jobs))
	}
	if st.FilesBad == 0 || st.FilesOK == 0 {
		t.Fatalf("flaky server should split outcomes, got ok=%d bad=%d", st.FilesOK, st.FilesBad)
	}
}

// TestSegmentedConcatenation sanity: segmented transfer over the fake server
// produces a correct, verifiable file (no checksum, just size).
func TestSegmentedConcatenation(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()
	dir := t.TempDir()
	j := Job{Name: "big", URL: fmt.Sprintf("%s/?size=%d", srv.URL, 64<<20), Dest: filepath.Join(dir, "big"), Size: 64 << 20}
	e := &Engine{Jobs: 4, Segments: 4, SegmentMin: 1, Verify: false, Quiet: true, HTTP: srv.Client(), MaxTries: 3}
	st := e.Run(context.Background(), []Job{j})
	if st.FilesBad != 0 {
		t.Fatalf("bad=%d", st.FilesBad)
	}
	fi, err := os.Stat(j.Dest)
	if err != nil || fi.Size() != 64<<20 {
		t.Fatalf("dest size=%v err=%v", fi, err)
	}
	// no leftover segment files
	leftovers, _ := filepath.Glob(filepath.Join(dir, "*.seg*"))
	if len(leftovers) > 0 {
		t.Fatalf("leftover segments: %v", leftovers)
	}
}

// TestResume verifies an existing complete destination is skipped.
func TestResume(t *testing.T) {
	srv := fakeServer(t)
	defer srv.Close()
	dir := t.TempDir()
	dest := filepath.Join(dir, "done")
	os.WriteFile(dest, make([]byte, 1<<20), 0o644)
	j := Job{Name: "done", URL: srv.URL, Dest: dest, Size: 1 << 20}
	e := &Engine{Jobs: 2, Verify: false, Quiet: true, HTTP: srv.Client(), MaxTries: 3}
	st := e.Run(context.Background(), []Job{j})
	if st.FilesOK != 1 || st.FilesBad != 0 {
		t.Fatalf("ok=%d bad=%d", st.FilesOK, st.FilesBad)
	}
}
