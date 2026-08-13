// Package engine implements parallel resumable downloads with optional
// segmented (aria2-style) transfer and integrity verification.
package engine

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Job is one file to download.
type Job struct {
	Name string // display name
	URL  string
	Dest string // final path
	Size int64  // expected size (0 = unknown)
	MD5  string
	SHA1 string
}

// Engine downloads jobs with a worker pool.
type Engine struct {
	Jobs       int
	Segments   int   // 0 = single stream
	SegmentMin int64 // min bytes for segmentation
	Verify     bool
	Quiet      bool
	HTTP       *http.Client
	Streamer   *CurlStreamer // if set, transfers go through curl (relay path)
	MaxTries   int
	// SegAuto picks the segment count per file size when the user did not
	// set --segments explicitly (8-40MB:2, >40MB:4, >120MB:6).
	SegAuto bool
	// Adaptive enables AIMD-lite pool sizing: start at Jobs, grow on rising
	// throughput, halve on failures, bounded by [MinJobs, MaxJobs].
	Adaptive bool
	MinJobs  int
	MaxJobs  int
}

// segmentsFor returns the auto segment count for a file of the given size
// (0 = single stream). Buckets tuned so segment overhead (per-range resolve
// + curl startup) stays small relative to transfer time.
func segmentsFor(size int64) int {
	switch {
	case size >= 120<<20:
		return 6
	case size >= 40<<20:
		return 4
	case size >= 8<<20:
		return 2
	}
	return 0
}

// Stats are aggregate transfer counters. FilesOK/FilesBad are atomic: the
// 1 Hz progress ticker reads them (render) while workers increment them.
type Stats struct {
	FilesOK  atomic.Int64
	FilesBad atomic.Int64
	Bytes    atomic.Int64
	Start    time.Time
	Mu       sync.Mutex
	Lines    []string
}

func (s *Stats) elapsed() time.Duration { return time.Since(s.Start) }

// Run downloads all jobs. Returns aggregate stats.
func (e *Engine) Run(ctx context.Context, jobs []Job) Stats {
	var st Stats
	st.Start = time.Now()
	if e.Jobs <= 0 {
		e.Jobs = 8
	}
	if e.MaxTries <= 0 {
		e.MaxTries = 3
	}
	// largest first: big files start early so their tail latency hides behind
	// the pool instead of extending the makespan.
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].Size > jobs[j].Size })

	minJ, maxJ := e.Jobs, e.Jobs
	if e.Adaptive {
		minJ = e.MinJobs
		if minJ <= 0 {
			minJ = e.Jobs / 2
		}
		if minJ < 2 {
			minJ = 2
		}
		if minJ > e.Jobs {
			minJ = e.Jobs
		}
		maxJ = e.MaxJobs
		if maxJ <= 0 {
			maxJ = e.Jobs * 4
		}
		if maxJ < minJ {
			maxJ = minJ
		}
		if maxJ > 64 {
			maxJ = 64
		}
	}
	// decFloor is the pool's hard shrink floor: even a sustained collapse
	// never takes the pool below max(2, initial/2), so one dead relay can't
	// collapse the download rate.
	decFloor := minJ
	if f := max(2, e.Jobs/2); f > decFloor {
		decFloor = f
	}

	jobCh := make(chan Job)
	// outcomeCh carries per-job results to the coordinator (sized so workers
	// never block on it, even after the coordinator stops draining).
	outcomeCh := make(chan jobOutcome, len(jobs)+2*maxJ)

	var adaptMu sync.Mutex
	cur := int32(e.Jobs)
	desired := int32(e.Jobs)
	window := make([]jobOutcome, 0, aimdWindow)
	var prevTp float64
	var decCooldown int // outcomes until the next pool DEC is allowed

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for j := range jobCh {
			start := time.Now()
			bytes, err := e.do(ctx, &st, j)
			if err != nil {
				st.Mu.Lock()
				st.FilesBad.Add(1)
				st.Lines = append(st.Lines, fmt.Sprintf("FAIL %s: %v", j.Name, err))
				st.Mu.Unlock()
			} else {
				st.FilesOK.Add(1)
			}
			outcomeCh <- jobOutcome{bytes: bytes, dur: time.Since(start), ok: err == nil}
			if e.Adaptive {
				adaptMu.Lock()
				if cur > desired { // pool shrunk: this worker exits when idle
					cur--
					adaptMu.Unlock()
					return
				}
				adaptMu.Unlock()
			}
		}
	}
	spawn := func() {
		wg.Add(1)
		go worker()
	}

	// progress ticker
	done := make(chan struct{})
	var tickerWG sync.WaitGroup
	if !e.Quiet {
		tickerWG.Add(1)
		go func() {
			defer tickerWG.Done()
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					e.render(&st)
				}
			}
		}()
	}

	for i := 0; i < e.Jobs; i++ {
		spawn()
	}

	// coordinator: feed jobs, adapt the pool as outcomes arrive. The select
	// interleaves dispatch with outcome handling so growth decisions happen
	// while jobs are still queued.
	sent := 0
	received := 0
	abort := false
	adapt := func(out jobOutcome) {
		if !e.Adaptive {
			return
		}
		adaptMu.Lock()
		defer adaptMu.Unlock()
		window = append(window, out)
		if len(window) > aimdWindow {
			window = window[1:]
		}
		var tp float64
		var action int
		tp, action, decCooldown = poolDecision(window, prevTp, decCooldown)
		prevTp = tp
		switch action {
		case poolDec:
			// sustained throughput collapse: halve, but never below decFloor
			d := decDesired(int(desired), decFloor)
			if d < int(desired) {
				desired = int32(d)
				debugf("pool DEC desired=%d cur=%d (sustained collapse)", desired, cur)
			}
		case poolInc:
			// additive increase only when throughput is rising and clean
			if int(desired) < maxJ {
				desired++
				debugf("pool INC desired=%d cur=%d tp=%.2f", desired, cur, tp)
			}
		}
		// grow the pool toward desired while jobs remain to be dispatched
		for cur < desired && !abort && sent < len(jobs) {
			cur++
			adaptMu.Unlock()
			spawn()
			adaptMu.Lock()
		}
	}
	for sent < len(jobs) && !abort {
		select {
		case out := <-outcomeCh:
			received++
			adapt(out)
		case jobCh <- jobs[sent]:
			sent++
		case <-ctx.Done():
			abort = true
		}
	}
	close(jobCh)
	// drain the outcomes of jobs already handed to workers
	for received < sent {
		out := <-outcomeCh
		received++
		adapt(out)
	}
	wg.Wait()
	close(done)
	tickerWG.Wait()
	if !e.Quiet {
		e.render(&st)
	}
	el := st.elapsed()
	secs := el.Seconds()
	b := st.Bytes.Load()
	mbps := 0.0
	if secs > 0 {
		mbps = float64(b) / 1e6 / secs
	}
	fmt.Fprintf(os.Stderr, "\ndone: %d ok, %d bad, %.1f MB in %s (%.2f MB/s)\n",
		st.FilesOK.Load(), st.FilesBad.Load(), float64(b)/1e6, el.Round(time.Millisecond), mbps)
	return st
}

// jobOutcome is one job's result, used by the adaptive pool (AIMD-lite).
// bytes = bytes accounted for the job (full size on success, partial on
// failure); ok = job succeeded.
type jobOutcome struct {
	bytes int64
	dur   time.Duration
	ok    bool
}

// aimdWindow is the sliding window of recent job results the adaptive pool
// uses to estimate throughput and failure rate. Sized up so individual job
// failures don't dominate the decision: one dead relay in a rotation yields
// roughly 50% failures, which must never trigger a DEC.
const aimdWindow = 10

// Pool actions returned by poolDecision.
const (
	poolNone = iota
	poolDec
	poolInc
)

// poolDecision evaluates the sliding window of recent job outcomes and
// returns the next pool action plus the window's aggregate throughput
// (MB/s). DEC fires only on a sustained throughput collapse — more than
// half the window failed AND throughput actually dropped vs the previous
// window — and at most once per aimdWindow outcomes. Individual failures
// (e.g. one dead relay in rotation) never shrink the pool.
func poolDecision(window []jobOutcome, prevTp float64, decCooldown int) (tp float64, action int, nextCooldown int) {
	nextCooldown = decCooldown
	if nextCooldown > 0 {
		nextCooldown--
	}
	var tb int64
	var td time.Duration
	fails := 0
	for _, w := range window {
		tb += w.bytes
		td += w.dur
		if !w.ok {
			fails++
		}
	}
	if td > 0 {
		tp = float64(tb) / 1e6 / td.Seconds()
	}
	if len(window) >= aimdWindow/2 && fails*2 > len(window) &&
		prevTp > 0 && tp < prevTp && nextCooldown == 0 {
		return tp, poolDec, aimdWindow
	}
	if prevTp > 0 && tp > prevTp*1.05 && len(window) >= aimdWindow {
		return tp, poolInc, nextCooldown
	}
	return tp, poolNone, nextCooldown
}

// decDesired halves desired but never below floor, so the adaptive pool
// can't collapse below max(2, initial/2) even under sustained failure. It
// never raises desired.
func decDesired(desired, floor int) int {
	if desired <= floor {
		return desired
	}
	d := desired / 2
	if d < floor {
		return floor
	}
	return d
}

func (e *Engine) render(st *Stats) {
	el := st.elapsed()
	secs := el.Seconds()
	b := st.Bytes.Load()
	var mbps float64
	if secs > 0 {
		mbps = float64(b) / 1e6 / secs
	}
	fmt.Fprintf(os.Stderr, "\r[%d ok / %d bad] %6.1f MB @ %6.2f MB/s  %s   ",
		st.FilesOK.Load(), st.FilesBad.Load(), float64(b)/1e6, mbps, el.Round(time.Second))
}

// do downloads one job to dest (via .part temp), verifying if requested.
// Returns the bytes accounted for the job (full size on success, partial on
// failure) so callers can count partial progress and feed the adaptive pool.
func (e *Engine) do(ctx context.Context, st *Stats, j Job) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(j.Dest), 0o755); err != nil {
		return 0, err
	}
	// already complete?
	if j.Size > 0 {
		if fi, err := os.Stat(j.Dest); err == nil && fi.Size() == j.Size {
			if e.Verify && (j.SHA1 != "" || j.MD5 != "") && e.check(j.Dest, j) {
				return j.Size, nil // complete & verified
			}
			if !e.Verify {
				return j.Size, nil
			}
		}
	}
	part := j.Dest + ".part"
	segs := e.segsFor(j.Size)
	if j.Size > 0 && segs > 1 && (e.SegmentMin <= 0 || j.Size >= e.SegmentMin) {
		if err := e.segmented(ctx, st, j, part); err != nil {
			pb := partialBytes(part) // count partial bytes on abort
			st.Bytes.Add(pb)
			return pb, err
		}
	} else {
		if err := e.single(ctx, st, j, part); err != nil {
			pb := partialBytes(part) // count partial bytes on abort
			st.Bytes.Add(pb)
			return pb, err
		}
	}
	// verify final file
	if e.Verify && (j.SHA1 != "" || j.MD5 != "") {
		if !e.check(part, j) {
			os.Remove(part)
			removeSegMetas(part)
			removeSegFiles(part)
			return 0, fmt.Errorf("checksum mismatch (sha1=%s md5=%s)", j.SHA1, j.MD5)
		}
	}
	if err := os.Rename(part, j.Dest); err != nil {
		return 0, err
	}
	st.Bytes.Add(j.Size) // count verified bytes
	if !e.Quiet {
		fmt.Fprintf(os.Stderr, "\n  ok %-60s (%d MB)\n", trunc(j.Name, 60), j.Size/1e6)
	}
	return j.Size, nil
}

// segsFor returns the segment count for a file: explicit e.Segments when set
// (or when SegAuto is off), otherwise the size-bucket default.
func (e *Engine) segsFor(size int64) int {
	if e.SegAuto {
		return segmentsFor(size)
	}
	return e.Segments
}

// partialBytes sums the bytes on disk for a failed download: the .part file
// plus any segment files still present after an aborted segmented transfer.
func partialBytes(part string) int64 {
	var total int64
	if fi, err := os.Stat(part); err == nil {
		total += fi.Size()
	}
	matches, _ := filepath.Glob(part + ".seg*")
	for _, m := range matches {
		if fi, err := os.Stat(m); err == nil {
			total += fi.Size()
		}
	}
	return total
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// copyBytes copies src to dst with a pooled 1MB buffer. io.Copy defaults to
// 32KB chunks; the larger buffer cuts syscalls and page-cache faults on the
// write/verify hot paths. Buffers are shared between concurrent segment
// writers via sync.Pool.
var copyBufPool = sync.Pool{New: func() any { b := make([]byte, 1<<20); return &b }}

func copyBytes(dst io.Writer, src io.Reader) (int64, error) {
	bp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bp)
	return io.CopyBuffer(dst, src, *bp)
}

// single downloads one stream with resume. Retries use exponential backoff
// (1s, 2s, 4s, ...) and the file aborts permanently after MaxTries
// consecutive failures.
func (e *Engine) single(ctx context.Context, st *Stats, j Job, part string) error {
	off := int64(0)
	if fi, err := os.Stat(part); err == nil {
		off = fi.Size()
	}
	// .part already holds the full size: skip the download entirely (avoids a
	// spurious 416 + curl -f failure on a complete .part) and let do() run
	// verify/rename on what we have.
	if j.Size > 0 && off >= j.Size {
		return nil
	}
	var lastErr error
	for try := 0; try < e.MaxTries; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(try)):
			}
		}
		if e.Streamer != nil {
			if err := e.Streamer.Fetch(ctx, j.URL, part, off, ""); err != nil {
				lastErr = err
				if fi, err2 := os.Stat(part); err2 == nil {
					off = fi.Size()
				}
				continue
			}
			if j.Size > 0 {
				fi, err2 := os.Stat(part)
				if err2 == nil && fi.Size() == j.Size {
					return nil
				}
				if err2 == nil {
					if fi.Size() > off {
						off = fi.Size()
					}
					lastErr = fmt.Errorf("short download: %d/%d", fi.Size(), j.Size)
					continue
				}
			}
			return nil
		}
		n, err := e.stream(ctx, st, j, part, off)
		if err == nil {
			// ensure full size
			if j.Size > 0 {
				fi, err2 := os.Stat(part)
				if err2 == nil && fi.Size() == j.Size {
					return nil
				}
				if err2 == nil {
					if fi.Size() > off {
						off = fi.Size()
					}
					lastErr = fmt.Errorf("short download: %d/%d", fi.Size(), j.Size)
					continue
				}
			}
			return nil
		}
		lastErr = err
		// recover partial progress for resume
		if fi, err2 := os.Stat(part); err2 == nil {
			off = fi.Size()
		}
		_ = n
	}
	if e.Streamer != nil {
		e.Streamer.MarkBadFor(j.URL) // MaxTries exhausted -> rotate relay off
	}
	return lastErr
}

// backoff returns the retry delay before attempt try (1-indexed): 1s, 2s,
// 4s, ... capped at 30s.
func backoff(try int) time.Duration {
	if try <= 1 {
		return time.Second
	}
	d := time.Duration(1<<(try-1)) * time.Second
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

// stream performs one ranged GET appending to part from offset off.
func (e *Engine) stream(ctx context.Context, st *Stats, j Job, part string, off int64) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.URL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "musget/1.0")
	if off > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
	}
	resp, err := e.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && off > 0 {
		// already complete
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if off > 0 && resp.StatusCode == http.StatusOK {
		// Server ignored the Range header and sent the full body from 0:
		// truncate the .part and restart cleanly instead of appending
		// duplicated data on top of the existing bytes.
		flags |= os.O_TRUNC
		off = 0
	}
	f, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if off > 0 {
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			return 0, err
		}
	}
	n, err := copyBytes(f, resp.Body)
	return n, err
}

// seg is one byte range of a download.
type seg struct {
	idx        int
	start, end int64
	file       string // per-segment temp file (curl relay path)
}

// segMetaPath returns the resume-progress sidecar for a direct-offset
// segment: the shared .part file's size can't tell per-segment progress when
// N goroutines write it concurrently, so each segment records its own byte
// count here (written after the data, so a crash can only under-report).
func segMetaPath(part string, i int) string {
	return fmt.Sprintf("%s.seg%d.meta", part, i)
}

// readSegMeta returns the bytes already written for this segment's range in
// the shared .part file, or 0 if the sidecar is missing/stale (wrong job).
func readSegMeta(meta string, want int64) int64 {
	b, err := os.ReadFile(meta)
	if err != nil {
		return 0
	}
	var w, have int64
	if _, err := fmt.Sscanf(string(b), "%d %d", &w, &have); err != nil {
		return 0
	}
	if w != want || have < 0 || have > want {
		return 0
	}
	return have
}

func writeSegMeta(meta string, want, have int64) {
	os.WriteFile(meta, []byte(fmt.Sprintf("%d %d\n", want, have)), 0o644)
}

// removeSegFiles deletes every per-segment temp file for a .part file
// (part.seg0, part.seg1, ...). On the curl relay path these are the segment
// data files that segOne/segBatch treat as valid when full-size; leaving them
// behind after a checksum-mismatch or failed concat would make a retry
// re-concatenate the same corrupt bytes forever.
func removeSegFiles(part string) {
	matches, _ := filepath.Glob(part + ".seg*")
	for _, m := range matches {
		os.Remove(m)
	}
}

// removeSegMetas deletes every resume sidecar for a .part file.
func removeSegMetas(part string) {
	d := filepath.Dir(part)
	base := filepath.Base(part)
	ents, err := os.ReadDir(d)
	if err != nil {
		return
	}
	for _, en := range ents {
		name := en.Name()
		if strings.HasPrefix(name, base+".seg") && strings.HasSuffix(name, ".meta") {
			os.Remove(filepath.Join(d, name))
		}
	}
}

// segmented splits the file into N ranges downloaded in parallel. On the Go
// HTTP path each segment is written straight into the .part file at its byte
// offset (no temp files, no concatenation pass); on the curl relay path curl
// writes per-segment temp files (curl cannot seek) that are then concatenated.
func (e *Engine) segmented(ctx context.Context, st *Stats, j Job, part string) error {
	segs := e.Segments
	if segs <= 0 {
		segs = 8
	}
	if j.Size <= 0 {
		return e.single(ctx, st, j, part)
	}
	chunk := j.Size / int64(segs)
	if chunk < 1 {
		return e.single(ctx, st, j, part)
	}
	// build segments
	var segsList []seg
	for i := 0; i < segs; i++ {
		s := int64(i) * chunk
		en := s + chunk - 1
		if i == segs-1 {
			en = j.Size - 1
		}
		if s > en {
			break
		}
		segsList = append(segsList, seg{i, s, en, fmt.Sprintf("%s.seg%d", part, i)})
	}
	direct := e.Streamer == nil // Go HTTP path: assemble in-place
	if direct {
		// If the .part file is gone, any leftover sidecars describe data that
		// no longer exists — start clean.
		if _, err := os.Stat(part); os.IsNotExist(err) {
			removeSegMetas(part)
		}
		// Pre-size .part (sparse) so concurrent range writes never contend on
		// extending the file and the final size is fixed up-front. Cheap and
		// safe: no blocks are allocated until a write lands there.
		if f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			f.Truncate(j.Size)
			f.Close()
		}
	}
	// download each segment: on the curl relay path with --parallel support,
	// ALL segments go through ONE curl -Z process (one resolve, one relay
	// connection); otherwise fall back to per-segment parallel curl/Go fetches.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	if !direct && e.Streamer.SupportsParallel() {
		firstErr = e.segBatch(ctx, j, segsList)
	} else {
		for _, sg := range segsList {
			wg.Add(1)
			go func(sg seg) {
				defer wg.Done()
				var err error
				if direct {
					err = e.segOneDirect(ctx, j, sg, part)
				} else {
					err = e.segOne(ctx, st, j, sg)
				}
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}(sg)
		}
		wg.Wait()
	}
	if firstErr != nil {
		return firstErr
	}
	if direct {
		// all segments already sit at their final offsets: nothing to assemble
		for _, sg := range segsList {
			os.Remove(segMetaPath(part, sg.idx))
		}
		return nil
	}
	// curl relay path: concatenate the per-segment files in order. Clean up
	// every seg file when the concat starts (deferred), so a failed concat can
	// never leave full-size .segN files behind for a retry to treat as valid —
	// safety over resume: a retry always re-downloads the segments.
	defer removeSegFiles(part)
	out, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	for _, sg := range segsList {
		in, err := os.Open(sg.file)
		if err != nil {
			return err
		}
		if _, err := copyBytes(out, in); err != nil {
			in.Close()
			return err
		}
		in.Close()
		os.Remove(sg.file)
	}
	return nil
}

// segBatch downloads ALL segments of one job in a single parallel curl
// (CurlStreamer.FetchBatch): resolve once, then one curl -Z process streams
// every segment from the same relay target. Any segment failure fails the
// whole batch, which is retried with exponential backoff; segments that
// already landed at their exact size are pruned on the next attempt (the
// per-segment temp files are the resume unit, as in segOne). After MaxTries
// the relay that served the job is rotated off via MarkBadFor.
func (e *Engine) segBatch(ctx context.Context, j Job, segs []seg) error {
	var lastErr error
	for try := 0; try < e.MaxTries; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(try)):
			}
		}
		// prune complete segments, restart partial ones (curl cannot append)
		specs := make([]SegSpec, 0, len(segs))
		allDone := true
		for _, sg := range segs {
			want := sg.end - sg.start + 1
			if fi, err := os.Stat(sg.file); err == nil && fi.Size() == want {
				continue
			}
			os.Remove(sg.file)
			allDone = false
			specs = append(specs, SegSpec{Dest: sg.file, Rng: fmt.Sprintf("%d-%d", sg.start, sg.end)})
		}
		if allDone {
			return nil
		}
		if err := e.Streamer.FetchBatch(ctx, j.URL, specs); err != nil {
			lastErr = err
			continue
		}
		// exact-size check: a segment that is short (cut stream) or oversize
		// (server ignored the Range and sent the full body) is not complete
		ok := true
		for _, sg := range segs {
			want := sg.end - sg.start + 1
			if fi, err := os.Stat(sg.file); err != nil || fi.Size() != want {
				ok = false
				lastErr = fmt.Errorf("segment short %d-%d", sg.start, sg.end)
			}
		}
		if ok {
			return nil
		}
	}
	e.Streamer.MarkBadFor(j.URL) // MaxTries exhausted -> rotate relay off
	return lastErr
}

// segOne downloads one segment through the curl relay into its own temp file
// (curl cannot seek into a shared file). Resume restarts the whole segment
// because curl appends from 0.
func (e *Engine) segOne(ctx context.Context, st *Stats, j Job, sg seg) error {
	want := sg.end - sg.start + 1
	have := int64(0)
	if fi, err := os.Stat(sg.file); err == nil {
		have = fi.Size()
	}
	if have >= want {
		return nil
	}
	// curl path: restart the whole segment on partial (curl cannot append)
	if have > 0 {
		os.Remove(sg.file)
	}
	rng := fmt.Sprintf("%d-%d", sg.start, sg.end)
	var lastErr error
	for try := 0; try < e.MaxTries; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(try)):
			}
		}
		if err := e.Streamer.Fetch(ctx, j.URL, sg.file, 0, rng); err != nil {
			lastErr = err
			os.Remove(sg.file)
			continue
		}
		if fi, err := os.Stat(sg.file); err == nil && fi.Size() >= want {
			return nil
		}
		os.Remove(sg.file)
		lastErr = fmt.Errorf("segment short %d-%d", sg.start, sg.end)
	}
	e.Streamer.MarkBadFor(j.URL) // MaxTries exhausted -> rotate relay off
	return lastErr
}

// segOneDirect downloads one segment straight into the shared .part file at
// its byte offset (direct-offset assembly). Per-segment resume progress comes
// from the .meta sidecar: the shared file's size can't distinguish segments.
func (e *Engine) segOneDirect(ctx context.Context, j Job, sg seg, part string) error {
	want := sg.end - sg.start + 1
	meta := segMetaPath(part, sg.idx)
	have := readSegMeta(meta, want)
	if have >= want {
		return nil
	}
	var lastErr error
	for try := 0; try < e.MaxTries; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(try)):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.URL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "musget/1.0")
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", sg.start+have, sg.end))
		resp, err := e.HTTP.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK {
			// Server ignored the Range: the body is the whole file. Restart
			// the segment and position inside the body at this segment's start.
			have = 0
			if _, err := io.CopyN(io.Discard, resp.Body, sg.start); err != nil {
				resp.Body.Close()
				lastErr = err
				continue
			}
		} else if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue
		}
		f, err := os.OpenFile(part, os.O_WRONLY, 0o644)
		if err != nil {
			resp.Body.Close()
			return err
		}
		if _, err := f.Seek(sg.start+have, io.SeekStart); err != nil {
			f.Close()
			resp.Body.Close()
			return err
		}
		// cap at the segment's remaining range so an oversized body can't
		// spill into the next segment's region
		n, cerr := copyBytes(f, io.LimitReader(resp.Body, want-have))
		f.Close()
		resp.Body.Close()
		have += n
		if have > want {
			have = want
		}
		writeSegMeta(meta, want, have)
		if cerr == nil && have >= want {
			return nil
		}
		if cerr == nil {
			lastErr = fmt.Errorf("segment short %d-%d", sg.start, sg.end)
			continue
		}
		lastErr = cerr
	}
	return lastErr
}

// check verifies a file against the job's hash.
func (e *Engine) check(path string, j Job) bool {
	var h hash.Hash
	hexWant := ""
	if j.SHA1 != "" {
		h = sha1.New()
		hexWant = j.SHA1
	} else {
		h = md5.New()
		hexWant = j.MD5
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	if _, err := copyBytes(h, f); err != nil {
		return false
	}
	got := hex.EncodeToString(h.Sum(nil))
	return strings.EqualFold(got, hexWant)
}
