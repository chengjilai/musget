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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Job is one file to download.
type Job struct {
	Name   string // display name
	URL    string
	Dest   string // final path
	Size   int64  // expected size (0 = unknown)
	MD5    string
	SHA1   string
}

// Engine downloads jobs with a worker pool.
type Engine struct {
	Jobs       int
	Segments   int           // 0 = single stream
	SegmentMin int64         // min bytes for segmentation
	Verify     bool
	Quiet      bool
	HTTP       *http.Client
	Streamer   *CurlStreamer // if set, transfers go through curl (relay path)
	MaxTries   int
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
		e.MaxTries = 4
	}
	jobCh := make(chan Job)
	var wg sync.WaitGroup
	for i := 0; i < e.Jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				err := e.do(ctx, &st, j)
				if err != nil {
					st.Mu.Lock()
					st.FilesBad++
					st.Lines = append(st.Lines, fmt.Sprintf("FAIL %s: %v", j.Name, err))
					st.Mu.Unlock()
				} else {
					st.FilesOK++
				}
			}
		}()
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
	for _, j := range jobs {
		select {
		case jobCh <- j:
		case <-ctx.Done():
			break
		}
	}
	close(jobCh)
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
func (e *Engine) do(ctx context.Context, st *Stats, j Job) error {
	if err := os.MkdirAll(filepath.Dir(j.Dest), 0o755); err != nil {
		return err
	}
	// already complete?
	if j.Size > 0 {
		if fi, err := os.Stat(j.Dest); err == nil && fi.Size() == j.Size {
			if e.Verify && (j.SHA1 != "" || j.MD5 != "") && e.check(j.Dest, j) {
				return nil // complete & verified
			}
			if !e.Verify {
				return nil
			}
		}
	}
	part := j.Dest + ".part"
	if j.Size > 0 && e.Segments > 1 && j.Size >= e.SegmentMin {
		if err := e.segmented(ctx, st, j, part); err != nil {
			return err
		}
	} else {
		if err := e.single(ctx, st, j, part); err != nil {
			return err
		}
	}
	// verify final file
	if e.Verify && (j.SHA1 != "" || j.MD5 != "") {
		if !e.check(part, j) {
			os.Remove(part)
			return fmt.Errorf("checksum mismatch (sha1=%s md5=%s)", j.SHA1, j.MD5)
		}
	}
	if err := os.Rename(part, j.Dest); err != nil {
		return err
	}
	st.Bytes.Add(j.Size) // count verified bytes
	if !e.Quiet {
		fmt.Fprintf(os.Stderr, "\n  ok %-60s (%d MB)\n", trunc(j.Name, 60), j.Size/1e6)
	}
	return nil
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// single downloads one stream with resume.
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
			case <-time.After(time.Duration(try) * time.Second):
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
	return lastErr
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
	if err == nil {
		st.Bytes.Add(n)
	}
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
func (e *Engine) segOne(ctx context.Context, st *Stats, j Job, sg struct{ start, end int64; file string }) error {
	want := sg.end - sg.start + 1
	have := int64(0)
	if fi, err := os.Stat(sg.file); err == nil {
		have = fi.Size()
	}
	if have >= want {
		st.Bytes.Add(want)
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
				time.Sleep(time.Duration(try) * time.Second)
			}
			if err := e.Streamer.Fetch(ctx, j.URL, sg.file, 0, rng); err != nil {
				lastErr = err
				os.Remove(sg.file)
				continue
			}
			if fi, err := os.Stat(sg.file); err == nil && fi.Size() >= want {
				st.Bytes.Add(want)
				return nil
			}
			os.Remove(sg.file)
			lastErr = fmt.Errorf("segment short %d-%d", sg.start, sg.end)
		}
		return lastErr
	}
	var lastErr error
	for try := 0; try < e.MaxTries; try++ {
		if try > 0 {
			time.Sleep(time.Duration(try) * time.Second)
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
		n, cerr := io.Copy(f, resp.Body)
		f.Close()
		resp.Body.Close()
		if cerr == nil {
			st.Bytes.Add(n)
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
