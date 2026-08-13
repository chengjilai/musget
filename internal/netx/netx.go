// Package netx builds tuned HTTP clients and handles proxy/direct detection.
package netx

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

// DefaultProxyCandidates tried in order when a host is unreachable directly.
var DefaultProxyCandidates = []string{
	"http://127.0.0.1:8888", // smart-proxy (WARP)
}

// TunedTransport returns an *http.Transport optimized for many parallel
// downloads: connection reuse, HTTP/2, per-host idle pool, generous timeouts.
func TunedTransport(proxyURL string) *http.Transport {
	var proxy func(*http.Request) (*url.URL, error)
	if proxyURL != "" {
		if u, err := url.Parse(proxyURL); err == nil {
			proxy = http.ProxyURL(u)
		}
	}
	return &http.Transport{
		Proxy:                 proxy,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		MaxConnsPerHost:       0, // unlimited: caller controls via worker pool
		IdleConnTimeout:       2 * time.Minute,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   20 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
}

// NewClient returns an http.Client with tuned transport.
func NewClient(proxyURL string, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: TunedTransport(proxyURL),
		Timeout:   timeout,
	}
}

// EnvProxy returns the first proxy found in standard env vars.
func EnvProxy() string {
	for _, k := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// Reachable checks whether url is reachable within timeout, returning the
// HTTP status (any 2xx/3xx/4xx counts as reachable; network errors = false).
func Reachable(client *http.Client, rawurl string, timeout time.Duration) (int, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawurl, nil)
	if err != nil {
		return 0, false
	}
	req.Header.Set("User-Agent", "musget/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	resp.Body.Close()
	return resp.StatusCode, true
}

// DetectProxy probes direct vs proxy for host and returns the proxy URL to use
// ("" if direct works). candidates are tried in order.
func DetectProxy(host string, timeout time.Duration) (string, error) {
	direct := NewClient("", timeout)
	if code, ok := Reachable(direct, "https://"+host, timeout); ok && code < 500 {
		return "", nil
	}
	proxies := []string{}
	for _, p := range DefaultProxyCandidates {
		proxies = append(proxies, p)
	}
	if ep := EnvProxy(); ep != "" {
		proxies = append([]string{ep}, proxies...)
	}
	for _, p := range proxies {
		c := NewClient(p, timeout)
		if code, ok := Reachable(c, "https://"+host, timeout); ok && code < 500 {
			return p, nil
		}
	}
	return "", fmt.Errorf("host %s unreachable directly or via any proxy", host)
}
