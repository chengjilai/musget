package engine

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"musget/internal/utlsx"
)

// Relay tracks the health of one CORS relay base. Free-tier relays are
// flaky/rate-limited, so a relay that misbehaves is put into a cooldown
// ("bad") period during which Pick() skips it in favor of others.
type Relay struct {
	Base  string  // relay base URL, e.g. https://proxy.cors.sh
	Speed float64 // MB/s measured at startup probe

	// failure history (guarded by RelaySet.mu): feeds Pick's weighted
	// selection so recently-flaky relays stay out of the hot path.
	lastFailTime     time.Time
	consecutiveFails int
	successCount     int64

	mu       sync.Mutex
	badUntil time.Time
}

func (r *Relay) badLocked(now time.Time) bool  { return now.Before(r.badUntil) }
func (r *Relay) markBadLocked(d time.Duration) { r.badUntil = time.Now().Add(d) }

// noteFailLocked records a failure event (any cooldown mark). Callers hold
// RelaySet.mu.
func (r *Relay) noteFailLocked() {
	r.consecutiveFails++
	r.lastFailTime = time.Now()
}

// noteSuccessLocked records a successful transfer: the streak resets so a
// relay that recovers rejoins the healthy rotation as an equal candidate.
// Callers hold RelaySet.mu.
func (r *Relay) noteSuccessLocked() {
	r.successCount++
	r.consecutiveFails = 0
	r.lastFailTime = time.Time{}
}

// pickScore returns a lower-is-better health score for r at time now.
// Relays with no failure history score 0; each consecutive failure adds 1,
// and a recent failure adds a decaying recency penalty (up to ~0.9 extra
// for a failure moments ago, fading over one minute) so a relay that just
// flaked ranks strictly worse than one that failed long ago. Callers hold
// RelaySet.mu.
func (r *Relay) pickScore(now time.Time) float64 {
	if r.consecutiveFails == 0 && r.lastFailTime.IsZero() {
		return 0
	}
	s := float64(r.consecutiveFails)
	if !r.lastFailTime.IsZero() {
		age := now.Sub(r.lastFailTime)
		if age < 0 {
			age = 0
		}
		const decay = time.Minute
		if age < decay {
			s += (1 - float64(age)/float64(decay)) * 0.9
		}
	}
	return s
}

// RelaySet manages a set of relays: round-robin rotation among healthy ones,
// cooldown-based failover, and per-relay startup speed measurements.
type RelaySet struct {
	mu       sync.Mutex
	relays   []*Relay
	next     int // round-robin cursor
	Cooldown time.Duration
	// SoftCooldown is used for transient relay misbehavior (HTTP 4xx/5xx
	// rate-limit pages, speed-limit stalls): the relay is rotated off right
	// away but may come back sooner than a hard connection-level failure.
	SoftCooldown time.Duration
}

// NewRelaySet builds a set from relay base URLs (may be empty).
func NewRelaySet(bases ...string) *RelaySet {
	s := &RelaySet{Cooldown: 60 * time.Second, SoftCooldown: 20 * time.Second}
	for _, b := range bases {
		b = strings.TrimRight(b, "/")
		if b != "" {
			s.relays = append(s.relays, &Relay{Base: b})
		}
	}
	return s
}

// Bases returns the relay bases in registration order.
func (s *RelaySet) Bases() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.relays))
	for i, r := range s.relays {
		out[i] = r.Base
	}
	return out
}

// SetSpeed records a relay's measured throughput (MB/s).
func (s *RelaySet) SetSpeed(base string, mbps float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.relays {
		if r.Base == base {
			r.Speed = mbps
		}
	}
}

// Speeds returns base -> MB/s for all relays.
func (s *RelaySet) Speeds() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[string]float64, len(s.relays))
	for _, r := range s.relays {
		m[r.Base] = r.Speed
	}
	return m
}

// MarkBad puts a relay into cooldown for the set's Cooldown period.
func (s *RelaySet) MarkBad(base string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.relays {
		if r.Base == base {
			r.markBadLocked(s.Cooldown)
			r.noteFailLocked()
		}
	}
}

// MarkBadSoft puts a relay into a shorter cooldown (SoftCooldown): used for
// transient failures like rate-limit pages and speed stalls so the relay
// rotates off immediately but can recover before the hard-failure cooldown
// expires.
func (s *RelaySet) MarkBadSoft(base string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.relays {
		if r.Base == base {
			r.markBadLocked(s.SoftCooldown)
			r.noteFailLocked()
		}
	}
}

// NoteSuccess records a successful transfer through relay base: it resets
// the consecutive-failure streak so a recovered relay rejoins the healthy
// rotation.
func (s *RelaySet) NoteSuccess(base string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.relays {
		if r.Base == base {
			r.noteSuccessLocked()
		}
	}
}

// Healthy returns bases not currently in cooldown.
func (s *RelaySet) Healthy() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []string
	for _, r := range s.relays {
		if !r.badLocked(now) {
			out = append(out, r.Base)
		}
	}
	return out
}

// AllBad reports whether every relay is in cooldown.
func (s *RelaySet) AllBad() bool { return len(s.Healthy()) == 0 }

// Pick returns the healthiest relay not in cooldown: relays with recent
// failures (pickScore) rank below clean ones, and among equal scores the
// faster relay wins; exact ties rotate round-robin. A small random nudge
// occasionally exercises a degraded-but-healthy relay so its history can
// refresh instead of starving forever. Returns nil if every relay is in
// cooldown.
func (s *RelaySet) Pick() *Relay {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.relays)
	if n == 0 {
		return nil
	}
	now := time.Now()
	best := -1
	var bestScore, bestSpeed float64
	for i := 0; i < n; i++ {
		idx := (s.next + i) % n
		r := s.relays[idx]
		if r.badLocked(now) {
			continue
		}
		sc := r.pickScore(now)
		if best < 0 || sc < bestScore || (sc == bestScore && r.Speed > bestSpeed) {
			best, bestScore, bestSpeed = idx, sc, r.Speed
		}
	}
	if best < 0 {
		return nil
	}
	// Tiny randomness: when the best relay is clean but another healthy
	// relay is degraded, ~8% of picks try a degraded one so a relay that has
	// recovered (or a dead one that should stay marked) gets exercised and
	// its history refreshes. A fully clean set rotates deterministically.
	if bestScore == 0 {
		var degraded []int
		for i := 0; i < n; i++ {
			idx := (s.next + i) % n
			r := s.relays[idx]
			if !r.badLocked(now) && r.pickScore(now) > 0 {
				degraded = append(degraded, idx)
			}
		}
		if len(degraded) > 0 && rand.Intn(100) < 8 {
			idx := degraded[rand.Intn(len(degraded))]
			s.next = (idx + 1) % n
			return s.relays[idx]
		}
	}
	s.next = (best + 1) % n
	return s.relays[best]
}

// CurlStreamer downloads via curl subprocesses, used for CORS-relay paths
// where Cloudflare rejects Go's TLS/HTTP2 fingerprint (403). curl performs
// the two-hop dance: GET the relay URL (no redirect follow) -> read the
// Location (dn node) -> curl the relay-wrapped Location for the real bytes.
type CurlStreamer struct {
	Set *RelaySet // healthy relays; Pick() rotates per job/segment
	UA  string
	// Fallback handles transfers when every relay is in cooldown (e.g. the
	// WARP smart-proxy path). May be nil; nil means the transfer fails.
	Fallback func(ctx context.Context, rawURL, dest string, off int64, rng string) error

	mu       sync.Mutex
	cache    map[string]string // raw archive.org URL -> dn-node Location (relay-independent)
	lastFail map[string]string // archive.org URL -> relay base of last failed fetch

	parallelOnce sync.Once
	parallelOK   bool
}

// SegSpec is one segment of a batched download: where curl writes it (Dest)
// and the byte range to fetch (Rng, "start-end" inclusive).
//
// curl's -r/-H options are GLOBAL even under -Z: every URL in one invocation
// gets the same range. So the batch cannot hand curl per-URL ranges directly;
// instead each segment URL points at a per-job local batching server with the
// range embedded in the path, and that server fetches the range from the
// resolved relay-wrapped target over a shared Chrome-fingerprint connection.
type SegSpec struct {
	Dest string
	Rng  string
}

// SupportsParallel reports whether the system curl was built with --parallel
// (-Z). The engine uses the batched path only when it is available and falls
// back to per-segment curl invocations otherwise.
func (c *CurlStreamer) SupportsParallel() bool {
	c.parallelOnce.Do(func() {
		out, err := exec.Command("curl", "--help", "all").Output()
		c.parallelOK = err == nil && strings.Contains(string(out), "--parallel")
	})
	return c.parallelOK
}

func NewCurlStreamer(bases ...string) *CurlStreamer {
	return &CurlStreamer{
		Set:      NewRelaySet(bases...),
		UA:       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
		cache:    make(map[string]string),
		lastFail: make(map[string]string),
	}
}

// resolve returns the relay-prefixed target URL for an archive.org URL via
// relay r: it asks the relay for headers (no redirect), extracts Location,
// and re-wraps it through the same relay. The cached dn-node Location is keyed
// by rawURL only (the Location is relay-independent), so relay rotation reuses
// the resolve instead of re-running the header round-trip per relay.
func (c *CurlStreamer) resolve(ctx context.Context, r *Relay, rawURL string) (string, error) {
	c.mu.Lock()
	if loc, ok := c.cache[rawURL]; ok {
		c.mu.Unlock()
		return r.Base + "/" + loc, nil
	}
	c.mu.Unlock()

	relayURL := r.Base + "/" + rawURL
	cmd := exec.CommandContext(ctx, "curl", "-sS", "-o", "/dev/null", "-D", "-",
		"-X", "GET", "--max-time", "90", "--connect-timeout", "15", "-A", c.UA, relayURL)
	cmd.Env = append(os.Environ(), "CURL_HOME=/tmp")
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		if RelayHardFail(stderr) {
			c.Set.MarkBad(r.Base)
		}
		return "", fmt.Errorf("resolve %s: %w", rawURL, err)
	}
	loc := ""
	status := ""
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "location:") {
			loc = strings.TrimSpace(line[len("location:"):])
		}
		if strings.HasPrefix(lower, "http/") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				status = parts[1]
			}
		}
	}
	// A download URL must redirect (302) to a dn node. A 4xx/5xx status is a
	// transient relay-level rate-limit/error page -> mark bad (soft) so retries
	// prefer the other relay; the relay can recover after the short cooldown. A
	// missing Location (e.g. a transient 200 with an error body) is left to the
	// engine's retry loop, which recovers quickly.
	if status != "" && (status[0] == '4' || status[0] == '5') {
		c.Set.MarkBadSoft(r.Base)
		debugf("resolve BAD %s status=%s url=%s", r.Base, status, rawURL)
		return "", fmt.Errorf("resolve %s: relay HTTP %s", rawURL, status)
	}
	if loc == "" {
		debugf("resolve SOFT %s no-Location status=%s url=%s", r.Base, status, rawURL)
		return "", fmt.Errorf("resolve %s: no Location header (relay HTTP %s)", rawURL, status)
	}
	target := r.Base + "/" + loc
	c.mu.Lock()
	c.cache[rawURL] = loc
	c.mu.Unlock()
	return target, nil
}

// Fetch downloads url to dest. If off>0, resume via curl -C. If rng is
// non-empty ("start-end"), issue a ranged request (segments). A relay is
// picked per call (round-robin over healthy relays, skipping cooldowns);
// hard failures mark the relay bad so later calls prefer others. When every
// relay is bad, Fallback (if set) takes over.
func (c *CurlStreamer) Fetch(ctx context.Context, rawURL, dest string, off int64, rng string) error {
	r := c.Set.Pick()
	if r == nil {
		if c.Fallback != nil {
			debugf("FALLBACK to WARP: %s off=%d rng=%q", rawURL, off, rng)
			return c.Fallback(ctx, rawURL, dest, off, rng)
		}
		return fmt.Errorf("all CORS relays unavailable (cooldown)")
	}
	target, err := c.resolve(ctx, r, rawURL)
	if err != nil {
		return err
	}
	args := []string{"-sS", "-f", "--retry", "2", "--retry-delay", "2",
		"--connect-timeout", "15", "--speed-limit", "1024", "--speed-time", "15",
		"--max-time", "1800", "-A", c.UA, "-o", dest}
	if off > 0 {
		args = append(args, "-C", fmt.Sprint(off))
	}
	if rng != "" {
		args = append(args, "-r", rng)
	}
	args = append(args, target)
	cmd := exec.CommandContext(ctx, "curl", args...)
	cmd.Env = append(os.Environ(), "CURL_HOME=/tmp")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	_, err = cmd.Output()
	if err != nil {
		c.mu.Lock()
		c.lastFail[rawURL] = r.Base
		c.mu.Unlock()
		if RelayStalled(stderr.String()) {
			// speed-limit abort: the relay is rate-limiting this stream, so
			// rotate it off now (soft) instead of retrying the same relay; the
			// engine's retry loop will pick another relay for the next attempt.
			c.Set.MarkBadSoft(r.Base)
			debugf("fetch STALL %s err=%v url=%s", r.Base, err, rawURL)
		} else if RelayHardFail(stderr.String()) {
			if RelayRateLimited(stderr.String()) {
				c.Set.MarkBadSoft(r.Base)
			} else {
				c.Set.MarkBad(r.Base)
			}
		}
		debugf("fetch ERR %s hard=%v err=%v stderr=%q url=%s", r.Base, RelayHardFail(stderr.String()), err, truncStr(stderr.String(), 120), rawURL)
		return fmt.Errorf("curl %s: %v (%s)", truncStr(target, 60), err, truncStr(stderr.String(), 160))
	}
	// A "successful" transfer that produced ~0 bytes (empty body) is a relay
	// glitch: don't NoteSuccess it or the relay stays "clean" on top while the
	// engine's retries keep burning on it. Soft-mark it so the next attempt
	// rotates elsewhere; the engine's short-download check handles the retry.
	if fi, err := os.Stat(dest); err != nil || fi.Size() <= off {
		c.Set.MarkBadSoft(r.Base)
		debugf("fetch EMPTY %s url=%s", r.Base, rawURL)
		return nil
	}
	c.Set.NoteSuccess(r.Base)
	return nil
}

// FetchBatch downloads all segments of one job in a SINGLE curl -Z process
// (--parallel), so the job pays one resolve and one relay connection instead
// of N fetches. curl cannot apply per-URL ranges (-r is global), so the
// batcher runs a per-job local HTTP server: each segment URL carries its
// range in the path, curl streams it from there, and the server fetches the
// range from the resolved relay-wrapped target over a shared
// Chrome-fingerprint connection (utls) — one TLS handshake multiplexed
// across every segment. A nonzero curl exit (any segment failed, e.g. the
// relay returned a rate-limit page) fails the whole batch; the engine retries
// with backoff and rotates the relay off via MarkBadFor after MaxTries.
func (c *CurlStreamer) FetchBatch(ctx context.Context, rawURL string, segs []SegSpec) error {
	if len(segs) == 0 {
		return nil
	}
	r := c.Set.Pick()
	if r == nil {
		if c.Fallback != nil {
			debugf("FALLBACK to WARP (batch %d segs): %s", len(segs), rawURL)
			for _, sg := range segs {
				if err := c.Fallback(ctx, rawURL, sg.Dest, 0, sg.Rng); err != nil {
					return err
				}
			}
			return nil
		}
		return fmt.Errorf("all CORS relays unavailable (cooldown)")
	}
	target, err := c.resolve(ctx, r, rawURL)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	bs := &batchServer{
		target: target,
		ua:     c.UA,
		relay:  r,
		set:    c.Set,
		client: newRelayClient(),
	}
	srv := &http.Server{Handler: bs}
	go srv.Serve(ln) //nolint:errcheck // closed via srv.Close below
	debugf("batch %d segs relay=%s target=%s", len(segs), r.Base, truncStr(target, 70))
	defer srv.Close()

	args := []string{"-sS", "-f", "-Z", "--parallel-max", fmt.Sprint(len(segs)),
		"--connect-timeout", "15", "-A", c.UA}
	for _, sg := range segs {
		args = append(args, "-o", sg.Dest, "http://"+ln.Addr().String()+"/r/"+sg.Rng)
	}
	cmd := exec.CommandContext(ctx, "curl", args...)
	cmd.Env = append(os.Environ(), "CURL_HOME=/tmp")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if _, err := cmd.Output(); err != nil {
		c.mu.Lock()
		c.lastFail[rawURL] = r.Base
		c.mu.Unlock()
		// Relay-health classification happens inside the batch server (it
		// sees the real backend error); curl's stderr only reflects the
		// localhost hop, so do not classify from it here.
		debugf("batch curl ERR %s err=%v stderr=%q url=%s", r.Base, err, truncStr(stderr.String(), 120), rawURL)
		return fmt.Errorf("parallel curl %s: %v (%s)", truncStr(target, 60), err, truncStr(stderr.String(), 160))
	}
	return nil
}

// batchServer is the per-job local HTTP server that hands curl per-segment
// ranges of one resolved relay target. All state here is per FetchBatch call,
// so concurrent jobs never share mutable state. Relay failover markings use
// the shared (mutex-protected) RelaySet exactly like Fetch does.
type batchServer struct {
	target string // relay-wrapped dn URL, resolved once per job
	ua     string
	relay  *Relay
	set    *RelaySet
	client *http.Client // shared backend client: one pooled connection per job
}

// progressReader records the last time bytes were observed so a watchdog can
// detect a stalled relay (mirrors curl's --speed-limit/--speed-time abort).
type progressReader struct {
	r    io.Reader
	mu   sync.Mutex
	last time.Time
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.mu.Lock()
		p.last = time.Now()
		p.mu.Unlock()
	}
	return n, err
}

func (b *batchServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// path: /r/<start>-<end>
	var start, end int64
	if _, err := fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/r/"), "%d-%d", &start, &end); err != nil || end < start {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	// Header guard: if the relay does not answer within 90s, abort. The body
	// phase has its own stall watchdog below (no flat deadline, matching the
	// single-stream path's --max-time 1800).
	hdDone := make(chan struct{})
	go func() {
		select {
		case <-time.After(90 * time.Second):
			cancel()
		case <-hdDone:
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.target, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("User-Agent", b.ua)
	resp, err := b.client.Do(req)
	close(hdDone)
	if err != nil {
		b.backendErr(err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		// 4xx/5xx from the relay = rate-limit/error page: rotate it off now
		// (soft) so retries prefer another relay; it recovers after the short
		// cooldown. 3xx is unexpected (resolve already followed it).
		if resp.StatusCode >= 400 {
			b.set.MarkBadSoft(b.relay.Base)
			debugf("batch RELAY %d %s url=%s", resp.StatusCode, b.relay.Base, b.target)
		}
		http.Error(w, fmt.Sprintf("relay HTTP %d", resp.StatusCode), http.StatusBadGateway)
		return
	}
	// A 200 means the server ignored the Range header (full body): stream it
	// anyway; the engine's exact-size check catches the oversize segment and
	// retries, exactly like the single-stream curl -r path.
	pr := &progressReader{r: resp.Body, last: time.Now()}
	// Body stall watchdog: abort (and rotate the relay off) if no bytes move
	// for 15s. Armed once the response headers have arrived.
	go func() {
		<-hdDone // headers arrived (channel closed above)
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pr.mu.Lock()
				last := pr.last
				pr.mu.Unlock()
				if time.Since(last) > 15*time.Second {
					b.set.MarkBadSoft(b.relay.Base)
					debugf("batch STALL %s url=%s", b.relay.Base, b.target)
					cancel() // aborts the backend request -> curl sees a broken stream
					return
				}
			}
		}
	}()
	if _, err := io.Copy(w, pr); err != nil {
		// aborted (stall or ctx): curl -f fails this transfer, batch exits
		// nonzero, engine retries. Nothing to write here.
		return
	}
}

// backendErr classifies a connection-level failure to the relay and marks it
// for failover, mirroring the single-stream path's RelayHardFail handling
// (rate-limit pages use the short soft cooldown, connection failures the
// hard one).
func (b *batchServer) backendErr(err error) {
	s := strings.ToLower(err.Error())
	for _, p := range []string{"no such host", "connection refused", "connection reset",
		"eof", "handshake", "stream error", "connection closed", "no h2", "timed out"} {
		if strings.Contains(s, p) {
			b.set.MarkBad(b.relay.Base)
			debugf("batch HARD %s err=%v url=%s", b.relay.Base, err, b.target)
			return
		}
	}
	debugf("batch ERR %s err=%v url=%s", b.relay.Base, err, b.target)
}

// newRelayClient returns the backend HTTP client for the relay-wrapped target.
// Go's native TLS fingerprint is rejected (403) by the relays' Cloudflare
// front, so TLS uses the repo's Chrome-fingerprint dialer (utlsx) and requests
// are multiplexed over one HTTP/2 connection — the handshake savings this
// batching exists for. Plain-HTTP targets (tests) use a stock transport.
func newRelayClient() *http.Client {
	h1 := &http.Transport{
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       2 * time.Minute,
		TLSHandshakeTimeout:   20 * time.Second,
		ResponseHeaderTimeout: 90 * time.Second,
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return utlsx.DialTLSContext(ctx, network, addr, nil)
		},
	}
	h2 := utlsx.Transport()
	return &http.Client{
		Transport: &hybridRT{h2: h2, h1: h1},
		// the relay-wrapped dn URL serves directly (no follow, like curl -f
		// without -L); a 3xx here is an error the engine retries.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// hybridRT uses the multiplexing HTTP/2 transport for https targets and a
// plain transport otherwise, falling back to plain TLS when the server does
// not negotiate HTTP/2.
type hybridRT struct {
	h2 http.RoundTripper
	h1 http.RoundTripper
}

func (rt *hybridRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "https" {
		return rt.h1.RoundTrip(req)
	}
	resp, err := rt.h2.RoundTrip(req)
	if err == nil {
		return resp, nil
	}
	// http2.Transport fails with "http2: unexpected ALPN protocol ..." when
	// the server does not negotiate h2: fall back to plain TLS (h1) so the
	// transfer still works. Match both "http2" and "http/2" spellings.
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "http2") || strings.Contains(s, "http/2") || strings.Contains(s, "alpn") {
		return rt.h1.RoundTrip(req) // no h2 support: retry over h1 (utls TLS)
	}
	return resp, err
}

// MarkBadFor marks the relay that last failed for rawURL as bad. Called by
// the engine after MaxTries are exhausted so repeated soft failures also
// rotate relays off. Safe to call concurrently; worst case a healthy relay
// gets a 60s cooldown.
func (c *CurlStreamer) MarkBadFor(rawURL string) {
	c.mu.Lock()
	base := c.lastFail[rawURL]
	c.mu.Unlock()
	if base != "" {
		c.Set.MarkBad(base)
	}
}

// RelayHardFail reports whether a curl failure indicates the relay itself is
// hard-down: HTTP 4xx/5xx (rate-limit / error page) or a connection-level
// failure. Transient glitches (empty responses, stalls, timeouts) are NOT
// hard: the engine's retry loop recovers from those in a try or two, and
// repeated failures are caught by MaxTries exhaustion -> MarkBadFor.
func RelayHardFail(stderr string) bool {
	for _, code := range []string{"403", "404", "429", "500", "502", "503", "504"} {
		if strings.Contains(stderr, "error: "+code) || strings.Contains(stderr, "HTTP "+code) ||
			strings.Contains(stderr, "response code: "+code) {
			return true
		}
	}
	for _, pat := range []string{"Empty reply from server", "Recv failure",
		"Connection refused", "Could not resolve host", "Failed to connect",
		"Connection reset by peer", "SSL connect error", "HTTP/2 stream 0 was not closed cleanly"} {
		if strings.Contains(stderr, pat) {
			return true
		}
	}
	return false
}

// RelayRateLimited reports whether a curl failure is specifically an HTTP
// rate-limit / error-page response (4xx/5xx from the relay). These are
// transient and clear quickly, so they use the shorter soft cooldown.
func RelayRateLimited(stderr string) bool {
	for _, code := range []string{"403", "404", "429", "500", "502", "503", "504"} {
		if strings.Contains(stderr, "error: "+code) || strings.Contains(stderr, "HTTP "+code) ||
			strings.Contains(stderr, "response code: "+code) {
			return true
		}
	}
	return false
}

// RelayStalled reports whether a curl failure is a speed-limit abort: the
// transfer dropped below --speed-limit for --speed-time seconds, which on a
// relay usually means per-IP rate-limiting kicked in. The engine reacts by
// rotating the relay off immediately and retrying via another relay.
func RelayStalled(stderr string) bool {
	return strings.Contains(stderr, "too slow") || strings.Contains(stderr, "Operation too slow")
}

func truncStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// debugf prints when MUSGET_DEBUG is set (relay failover diagnostics).
func debugf(format string, args ...any) {
	if os.Getenv("MUSGET_DEBUG") != "" {
		fmt.Fprintf(os.Stderr, "[dbg] "+format+"\n", args...)
	}
}
