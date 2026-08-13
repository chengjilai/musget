package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"musget/internal/archivex"
	"musget/internal/corsx"
	"musget/internal/engine"
	"musget/internal/netx"
)

type mode int

const (
	modeDirect mode = iota
	modeCORS
	modeProxy
)

func (m mode) String() string {
	switch m {
	case modeCORS:
		return "cors-relay"
	case modeProxy:
		return "http-proxy"
	default:
		return "direct"
	}
}

// relayFlag is set by --relay in get/search; forces a single relay.
var relayFlag string

// relayProbe is one relay's startup health-check result.
type relayProbe struct {
	base  string
	speed float64 // MB/s
	ok    bool
}

// pickArchiveClient chooses the fastest reachable path to archive.org:
// direct, then a CORS relay (mainland-China friendly, ~15 MB/s), then an
// HTTP proxy (e.g. WARP smart-proxy, ~1 MB/s). In CORS mode it returns the
// healthy relay bases ordered fastest-first for per-job rotation.
func pickArchiveClient(ctx context.Context) (client *archivex.Client, m mode, bases []string, err error) {
	if relayFlag != "" {
		base := corsx.CanonicalBase(relayFlag)
		if base == "" {
			return nil, 0, nil, fmt.Errorf("unknown relay %q (want URL or one of: %s)",
				relayFlag, strings.Join(relayNames(), ", "))
		}
		return archivex.NewDirectClient(), modeCORS, []string{base}, nil
	}
	if strings.HasPrefix(proxyFlag, "cors:") {
		base := strings.TrimPrefix(proxyFlag, "cors:")
		if base == "" {
			base = corsx.Bases[0]
		}
		return archivex.NewDirectClient(), modeCORS, []string{base}, nil
	}
	if proxyFlag != "" {
		return archivex.NewProxyClient(proxyFlag), modeProxy, nil, nil
	}
	direct := archivex.NewDirectClient()
	if code, ok := netx.Reachable(direct.HTTP, "https://archive.org/metadata/faure-nocturnes-1-8", 8*time.Second); ok && code < 500 {
		return direct, modeDirect, nil, nil
	}
	// relay reachable? probe both with a ranged request, pick the fastest.
	if b := relayBases(ctx); len(b) > 0 {
		return archivex.NewDirectClient(), modeCORS, b, nil
	}
	if p := netx.EnvProxy(); p != "" {
		return archivex.NewProxyClient(p), modeProxy, nil, nil
	}
	if p, err := netx.DetectProxy("archive.org", 10*time.Second); err == nil && p != "" {
		return archivex.NewProxyClient(p), modeProxy, nil, nil
	}
	return direct, modeDirect, nil, nil
}

// relayNames returns the --relay aliases for error messages.
func relayNames() []string {
	out := make([]string, 0, len(corsx.ShortNames))
	for k := range corsx.ShortNames {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// probeRelays checks every relay base with a small ranged request
// (-r 0-1048575, 5s timeout), measuring bytes/sec, and returns the healthy
// ones sorted fastest-first. curl is used because the relays' Cloudflare
// front rejects Go's TLS fingerprint. All relays are probed concurrently.
func probeRelays(ctx context.Context, bases []string) []relayProbe {
	probes := make([]relayProbe, len(bases))
	var wg sync.WaitGroup
	for i, base := range bases {
		wg.Add(1)
		go func(i int, base string) {
			defer wg.Done()
			probes[i] = probeOneRelay(ctx, base)
		}(i, base)
	}
	wg.Wait()
	ok := probes[:0]
	for _, p := range probes {
		if p.ok {
			ok = append(ok, p)
		}
	}
	sort.SliceStable(ok, func(i, j int) bool { return ok[i].speed > ok[j].speed })
	return ok
}

func probeOneRelay(ctx context.Context, base string) relayProbe {
	p := relayProbe{base: base}
	// a stable, non-redirecting archive.org endpoint big enough to
	// measure throughput over ~1MB.
	u := base + "/https://archive.org/advancedsearch.php?q=date:[1900-01-01+TO+1901-01-01]&rows=1000&output=json"
	cmd := exec.CommandContext(ctx, "curl", "-sS", "-g", "-o", "/dev/null",
		"-w", "%{http_code} %{size_download} %{speed_download}",
		"--connect-timeout", "5", "--max-time", "10", "-A", "curl",
		"-r", "0-1048575", u)
	out, err := cmd.Output()
	if err != nil {
		return p // unreachable / timed out
	}
	f := strings.Fields(strings.TrimSpace(string(out)))
	if len(f) < 3 {
		return p
	}
	code := f[0]
	if code != "200" && code != "206" {
		return p
	}
	var size int64
	var speed float64
	fmt.Sscanf(f[1], "%d", &size)
	fmt.Sscanf(f[2], "%f", &speed)
	if size <= 0 {
		return p
	}
	p.ok = true
	p.speed = speed / 1e6
	return p
}

// relayBases returns healthy relay bases ordered fastest-first, or nil.
func relayBases(ctx context.Context) []string {
	probes := probeRelays(ctx, corsx.Bases)
	var out []string
	for _, p := range probes {
		if p.ok {
			out = append(out, p.base)
		}
	}
	return out
}

// curlFetch returns an API fetch function that goes through the relay via
// curl (Cloudflare rejects Go's TLS fingerprint on the relay). Relays are
// rotated, bad ones skipped, and when every relay fails it falls back to the
// WARP smart-proxy (also via curl).
func curlFetch(bases []string) func(ctx context.Context, u string) ([]byte, error) {
	set := engine.NewRelaySet(bases...)
	return func(ctx context.Context, u string) ([]byte, error) {
		for i := 0; i <= len(bases); i++ {
			r := set.Pick()
			if r == nil {
				break // all relays in cooldown
			}
			cmd := exec.CommandContext(ctx, "curl", "-sS", "-f", "--max-time", "90",
				"--connect-timeout", "15", "-A", "curl", r.Base+"/"+u)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				if engine.RelayHardFail(stderr.String()) {
					set.MarkBad(r.Base)
				}
				continue // try next healthy relay
			}
			return out, nil
		}
		// every relay bad/unreachable: fall back to the WARP proxy
		if p := detectWarpProxy(); p != "" {
			cmd := exec.CommandContext(ctx, "curl", "-sS", "-f", "-L", "--max-time", "90",
				"--connect-timeout", "15", "-A", "curl", "-x", p, u)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if out, err := cmd.Output(); err == nil {
				return out, nil
			}
		}
		return nil, fmt.Errorf("relay fetch failed for all relays")
	}
}

// curlStreamer returns a relay streamer over the healthy bases (rotating per
// job/segment), with the WARP proxy as the last-resort fallback when every
// relay is in cooldown.
func curlStreamer(bases []string) *engine.CurlStreamer {
	cs := engine.NewCurlStreamer(bases...)
	cs.Fallback = warpFallback
	return cs
}

var (
	warpOnce  sync.Once
	warpProxy string
)

// detectWarpProxy returns the WARP smart-proxy URL, detecting it lazily on
// first use (env vars, then smart-proxy candidate) and caching the result.
func detectWarpProxy() string {
	warpOnce.Do(func() {
		if p := netx.EnvProxy(); p != "" {
			warpProxy = p
			return
		}
		if p, err := netx.DetectProxy("archive.org", 10*time.Second); err == nil && p != "" {
			warpProxy = p
		}
	})
	return warpProxy
}

// warpFallback downloads through the WARP smart-proxy with curl, used only
// when all CORS relays are in cooldown.
func warpFallback(ctx context.Context, rawURL, dest string, off int64, rng string) error {
	p := detectWarpProxy()
	if p == "" {
		return fmt.Errorf("all CORS relays unavailable and no WARP proxy found")
	}
	args := []string{"-sS", "-f", "-L", "--retry", "2", "--retry-delay", "2",
		"--connect-timeout", "15", "--speed-limit", "1024", "--speed-time", "15",
		"--max-time", "0", "-A", "curl", "-x", p, "-o", dest}
	if off > 0 {
		args = append(args, "-C", fmt.Sprint(off))
	}
	if rng != "" {
		args = append(args, "-r", rng)
	}
	args = append(args, rawURL)
	cmd := exec.CommandContext(ctx, "curl", args...)
	cmd.Env = append(os.Environ(), "CURL_HOME=/tmp")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if _, err := cmd.Output(); err != nil {
		return fmt.Errorf("warp fallback curl %s: %v (%s)",
			truncate(rawURL, 60), err, truncate(stderr.String(), 160))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
