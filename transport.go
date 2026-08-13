package main

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
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

// pickArchiveClient chooses the fastest reachable path to archive.org:
// direct, then a CORS relay (mainland-China friendly, ~15 MB/s), then an
// HTTP proxy (e.g. WARP smart-proxy, ~1 MB/s).
func pickArchiveClient(ctx context.Context) (client *archivex.Client, m mode, corsBase string, err error) {
	if strings.HasPrefix(proxyFlag, "cors:") {
		return archivex.NewDirectClient(), modeCORS, strings.TrimPrefix(proxyFlag, "cors:"), nil
	}
	if proxyFlag != "" {
		return archivex.NewProxyClient(proxyFlag), modeProxy, "", nil
	}
	direct := archivex.NewDirectClient()
	if code, ok := netx.Reachable(direct.HTTP, "https://archive.org/metadata/faure-nocturnes-1-8", 8*time.Second); ok && code < 500 {
		return direct, modeDirect, "", nil
	}
	// relay reachable? verify with curl (Go TLS is 403'd by Cloudflare)
	if base := checkRelayViaCurl(ctx); base != "" {
		return archivex.NewDirectClient(), modeCORS, base, nil
	}
	if p := netx.EnvProxy(); p != "" {
		return archivex.NewProxyClient(p), modeProxy, "", nil
	}
	if p, err := netx.DetectProxy("archive.org", 10*time.Second); err == nil && p != "" {
		return archivex.NewProxyClient(p), modeProxy, "", nil
	}
	return direct, modeDirect, "", nil
}

// curlFetch returns an API fetch function that goes through the relay via
// curl (Cloudflare rejects Go's TLS fingerprint on the relay).
func curlFetch(corsBase string) func(ctx context.Context, u string) ([]byte, error) {
	return func(ctx context.Context, u string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "curl", "-sS", "-f", "--max-time", "90",
			"-A", "curl", corsBase+"/"+u)
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("relay fetch: %w", err)
		}
		return out, nil
	}
}

// curlStreamer returns a streamer bound to the relay base.
func curlStreamer(corsBase string) *engine.CurlStreamer {
	return engine.NewCurlStreamer(corsBase)
}

// checkRelayViaCurl tests CORS relays with curl (Go's TLS fingerprint is
// rejected by their Cloudflare front), returning the first working base.
func checkRelayViaCurl(ctx context.Context) string {
	for _, base := range corsx.Bases {
		cmd := exec.CommandContext(ctx, "curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
			"--max-time", "20", "-A", "curl", base+"/https://archive.org/metadata/faure-nocturnes-1-8")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) == "200" {
			return base
		}
	}
	return ""
}
