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

// Stats are aggregate transfer counters.
type Stats struct {
	FilesOK  int
	FilesBad int
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

	jobCh := make(chan Job)
	// outcomeCh carries per-job results to the coordinator (sized so workers
	// never block on it, even after the coordinator stops draining).
	outcomeCh := make(chan jobOutcome, len(jobs)+2*maxJ)

	var adaptMu sync.Mutex
	cur := int32(e.Jobs)
	desired := int32(e.Jobs)
	window := make([]jobOutcome, 0, aimdWindow)
	var prevTp float64

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for j := range jobCh {
			start := time.Now()
			bytes, err := e.do(ctx, &st, j)
			if err != nil {
				st.Mu.Lock()
				st.FilesBad++
				st.Lines = append(st.Lines, fmt.Sprintf("FAIL %s: %v", j.Name, err))
				st.Mu.Unlock()
			} else {
				st.FilesOK++
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
		tp := 0.0
		if td > 0 {
			tp = float64(tb) / 1e6 / td.Seconds()
		}
		rising := prevTp > 0 && tp > prevTp*1.05
		prevTp = tp
		if fails > 0 {
			// AIMD multiplicative decrease on failure, bounded below
			d := int(desired) / 2
			if d < minJ {
				d = minJ
			}
			desired = int32(d)
			debugf("pool DEC desired=%d cur=%d (fail)", desired, cur)
		} else if rising && len(window) >= aimdWindow && int(desired) < maxJ {
			// additive increase only when throughput is rising and clean
			desired++
			debugf("pool INC desired=%d cur=%d tp=%.2f", desired, cur, tp)
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
		st.FilesOK, st.FilesBad, float64(b)/1e6, el.Round(time.Millisecond), mbps)
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
// uses to estimate throughput and failure rate.
const aimdWindow = 5

func (e *Engine) render(st *Stats) {
	el := st.elapsed()
	secs := el.Seconds()
	b := st.Bytes.Load()
	var mbps float64
	if secs > 0 {
		mbps = float64(b) / 1e6 / secs
	}
	fmt.Fprintf(os.Stderr, "\r[%d ok / %d bad] %6.1f MB @ %6.2f MB/s  %s   ",
		st.FilesOK, st.FilesBad, float64(b)/1e6, mbps, el.Round(time.Second))
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

// single downloads one stream with resume. Retries use exponential backoff
// (1s, 2s, 4s, ...) and the file aborts permanently after MaxTries
// consecutive failures.
func (e *Engine) single(ctx context.Context, st *Stats, j Job, part string) error {
	off := int64(0)
	if fi, err := os.Stat(part); err == nil {
		off = fi.Size()
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
	f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if off > 0 {
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			return 0, err
		}
	}
	n, err := io.Copy(f, resp.Body)
	return n, err
}

// segmented splits the file into N ranges downloaded in parallel.
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
	type seg struct {
		start, end int64
		file       string
	}
	var segsList []seg
	for i := 0; i < segs; i++ {
		s := int64(i) * chunk
		e := s + chunk - 1
		if i == segs-1 {
			e = j.Size - 1
		}
		if s > e {
			break
		}
		segsList = append(segsList, seg{s, e, fmt.Sprintf("%s.seg%d", part, i)})
	}
	// download each segment (with per-segment resume via file size)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, sg := range segsList {
		wg.Add(1)
		go func(sg seg) {
			defer wg.Done()
			if err := e.segOne(ctx, st, j, sg); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(sg)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	// concatenate in order
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
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			return err
		}
		in.Close()
		os.Remove(sg.file)
	}
	return nil
}

// segOne downloads one segment; resumes by checking the partial segment size.
func (e *Engine) segOne(ctx context.Context, st *Stats, j Job, sg struct {
	start, end int64
	file       string
}) error {
	want := sg.end - sg.start + 1
	have := int64(0)
	if fi, err := os.Stat(sg.file); err == nil {
		have = fi.Size()
	}
	if have >= want {
		return nil
	}
	if e.Streamer != nil {
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
			// server ignored range: re-download whole segment
			have = 0
			os.Remove(sg.file)
		} else if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue
		}
		f, err := os.OpenFile(sg.file, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			resp.Body.Close()
			return err
		}
		if have > 0 {
			f.Seek(0, io.SeekEnd)
		}
		_, cerr := io.Copy(f, resp.Body)
		f.Close()
		resp.Body.Close()
		if cerr == nil {
			if fi, err := os.Stat(sg.file); err == nil && fi.Size() >= want {
				return nil
			}
			if fi, err := os.Stat(sg.file); err == nil {
				have = fi.Size()
			}
			continue
		}
		lastErr = cerr
		if fi, err := os.Stat(sg.file); err == nil {
			have = fi.Size()
		}
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
	if _, err := io.Copy(h, f); err != nil {
		return false
	}
	got := hex.EncodeToString(h.Sum(nil))
	return strings.EqualFold(got, hexWant)
}
