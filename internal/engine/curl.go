package engine

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// CurlStreamer downloads via curl subprocesses, used for CORS-relay paths
// where Cloudflare rejects Go's TLS/HTTP2 fingerprint (403). curl performs
// the two-hop dance: GET the relay URL (no redirect follow) -> read the
// Location (dn node) -> curl the relay-wrapped Location for the real bytes.
type CurlStreamer struct {
	Base  string // relay base, e.g. https://proxy.cors.sh
	UA    string
	mu    sync.Mutex
	cache map[string]string // archive.org URL -> resolved relay target
}

func NewCurlStreamer(base string) *CurlStreamer {
	return &CurlStreamer{
		Base:  strings.TrimRight(base, "/"),
		UA:    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
		cache: make(map[string]string),
	}
}

// resolve returns the relay-prefixed target URL for an archive.org URL:
// it asks the relay for headers (no redirect), extracts Location, and
// re-wraps it through the relay.
func (c *CurlStreamer) resolve(ctx context.Context, rawURL string) (string, error) {
	c.mu.Lock()
	if v, ok := c.cache[rawURL]; ok {
		c.mu.Unlock()
		return v, nil
	}
	c.mu.Unlock()

	relayURL := c.Base + "/" + rawURL
	cmd := exec.CommandContext(ctx, "curl", "-sS", "-o", "/dev/null", "-D", "-",
		"-X", "GET", "--max-time", "90", "-A", c.UA, relayURL)
	cmd.Env = append(os.Environ(), "CURL_HOME=/tmp")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", rawURL, err)
	}
	loc := ""
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(strings.ToLower(line), "location:") {
			loc = strings.TrimSpace(line[len("location:"):])
		}
	}
	if loc == "" {
		return "", fmt.Errorf("resolve %s: no Location header", rawURL)
	}
	target := c.Base + "/" + loc
	c.mu.Lock()
	c.cache[rawURL] = target
	c.mu.Unlock()
	return target, nil
}

// Fetch downloads url to dest. If off>0, resume via curl -C. If rng is
// non-empty ("start-end"), issue a ranged request (segments).
func (c *CurlStreamer) Fetch(ctx context.Context, rawURL, dest string, off int64, rng string) error {
	target, err := c.resolve(ctx, rawURL)
	if err != nil {
		return err
	}
	args := []string{"-sS", "-f", "--retry", "2", "--retry-delay", "2",
		"--max-time", "0", "-A", c.UA, "-o", dest}
	if off > 0 {
		args = append(args, "-C", fmt.Sprint(off))
	}
	if rng != "" {
		args = append(args, "-r", rng)
	}
	args = append(args, target)
	cmd := exec.CommandContext(ctx, "curl", args...)
	cmd.Env = append(os.Environ(), "CURL_HOME=/tmp")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("curl %s: %v (%s)", target[:60], err, truncStr(string(out), 160))
	}
	return nil
}

func truncStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
