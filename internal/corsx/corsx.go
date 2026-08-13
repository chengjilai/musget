// Package corsx implements a RoundTripper that routes requests through a
// CORS-style relay (e.g. proxy.cors.sh) which can reach GFW-blocked hosts
// from mainland China at full speed. Redirects to other blocked hosts are
// re-wrapped through the relay automatically, so callers see one stream.
package corsx

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Bases are tried in order when choosing a relay.
var Bases = []string{
	"https://proxy.cors.sh",
	"https://cors.eu.org",
}

// Transport wraps an inner transport and relays requests via base.
type Transport struct {
	Base    string
	Inner   http.RoundTripper
	Timeout time.Duration
}

// NewTransport returns a relay transport using base; inner may be nil (uses
// http.DefaultTransport clone).
func NewTransport(base string, inner http.RoundTripper) *Transport {
	if inner == nil {
		inner = &http.Transport{
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       2 * time.Minute,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   20 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		}
	}
	return &Transport{Base: strings.TrimRight(base, "/"), Inner: inner, Timeout: 0}
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// up to 4 hops: original -> relay -> dn node -> relay
	for hop := 0; hop < 4; hop++ {
		u := t.Base + "/" + req.URL.String()
		rr := req.Clone(req.Context())
		ru, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		rr.URL = ru
		rr.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
		resp, err := t.Inner.RoundTrip(rr)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			loc := resp.Header.Get("Location")
			resp.Body.Close()
			if loc == "" {
				return resp, nil
			}
			lu, err := url.Parse(loc)
			if err != nil {
				return resp, nil
			}
			// re-wrap the redirect target through the relay
			req.URL = lu
			continue
		}
		// strip the relay's own headers that could confuse range requests
		resp.Header.Del("Access-Control-Allow-Origin")
		resp.Header.Del("Access-Control-Allow-Methods")
		return resp, nil
	}
	return nil, fmt.Errorf("corsx: too many redirect hops")
}

// Check returns the first relay base that can reach archive.org, or "".
func Check(ctx context.Context, timeout time.Duration) string {
	for _, base := range Bases {
		c := &http.Client{Transport: NewTransport(base, nil), Timeout: timeout}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"https://archive.org/metadata/faure-nocturnes-1-8", nil)
		if err != nil {
			continue
		}
		resp, err := c.Do(req)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return base
			}
		}
	}
	return ""
}
