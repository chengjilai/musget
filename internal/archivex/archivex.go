// Package archivex is a minimal archive.org client (search + metadata).
package archivex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"musget/internal/corsx"
)

const Base = "https://archive.org"

// Client talks to archive.org. Transport may be a custom RoundTripper
// (e.g. corsx relay or WARP proxy wrapper). Fetch overrides the transport
// for API calls (used when the relay rejects Go's TLS fingerprint).
type Client struct {
	HTTP  *http.Client
	Fetch func(ctx context.Context, u string) ([]byte, error)
}

// NewDirectClient builds a client that talks to archive.org directly.
func NewDirectClient() *Client {
	return &Client{HTTP: &http.Client{Transport: tunedTransport(""), Timeout: 120 * time.Second}}
}

// NewProxyClient builds a client through the given HTTP proxy.
func NewProxyClient(proxy string) *Client {
	return &Client{HTTP: &http.Client{Transport: tunedTransport(proxy), Timeout: 120 * time.Second}}
}

// NewCORSClient builds a client routed through a CORS relay (corsx base).
func NewCORSClient(base string) *Client {
	tr := corsx.NewTransport(base, tunedTransport(""))
	return &Client{HTTP: &http.Client{Transport: tr, Timeout: 0}}
}

func tunedTransport(proxy string) *http.Transport {
	var tr *http.Transport
	if proxy != "" {
		tr = &http.Transport{
			ForceAttemptHTTP2:    true,
			MaxIdleConns:         256,
			MaxIdleConnsPerHost:  64,
			IdleConnTimeout:      2 * time.Minute,
			TLSHandshakeTimeout:  10 * time.Second,
			ResponseHeaderTimeout: 90 * time.Second,
		}
		if u, err := url.Parse(proxy); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	} else {
		tr = &http.Transport{
			ForceAttemptHTTP2:    true,
			MaxIdleConns:         256,
			MaxIdleConnsPerHost:  64,
			IdleConnTimeout:      2 * time.Minute,
			TLSHandshakeTimeout:  10 * time.Second,
			ResponseHeaderTimeout: 90 * time.Second,
		}
	}
	return tr
}

// Result is one search hit.
type Result struct {
	Identifier     string   `json:"identifier"`
	Title          string   `json:"title"`
	Description    any      `json:"description"`
	Mediatype      string   `json:"mediatype"`
	Collection     []string `json:"collection"`
	DownloadCount  int      `json:"downloads"`
	Year           any      `json:"year"`
	Creator        any      `json:"creator"`
	AccessRestricted string `json:"access-restricted-item"`
}


// Str converts any archive.org scalar-or-array metadata field to a string.
func (r *Result) Str(f any) string {
	switch v := f.(type) {
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "; ")
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}
// Search queries advancedsearch.php.
func (c *Client) Search(ctx context.Context, query string, limit int) ([]Result, error) {
	if limit <= 0 {
		limit = 20
	}
	q := url.Values{}
	q.Set("q", query)
	q.Set("fl[]", "identifier,title,description,mediatype,collection,downloads,year,creator,access-restricted-item")
	q.Set("rows", fmt.Sprint(limit))
	q.Set("output", "json")
	u := Base + "/advancedsearch.php?" + q.Encode()
	body, err := c.get(ctx, u)
	if err != nil {
		return nil, err
	}
	var out struct {
		Response struct {
			Docs []Result `json:"docs"`
		} `json:"response"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("bad search json: %w", err)
	}
	return out.Response.Docs, nil
}

// File is one file inside an item.
type File struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Format string `json:"format"`
	Source string `json:"source"`
	MD5    string `json:"md5"`
	SHA1   string `json:"sha1"`
	Length string `json:"length"`
}

// Item is an archive.org item with metadata + files.
type Item struct {
	Identifier string
	Title      string
	Mediatype  string
	Metadata   map[string]any
	Files      []File
	Server     string
}

// Item fetches /metadata/<id>.
func (c *Client) Item(ctx context.Context, id string) (*Item, error) {
	body, err := c.get(ctx, Base+"/metadata/"+url.PathEscape(id))
	if err != nil {
		return nil, err
	}
	var raw struct {
		Metadata map[string]any `json:"metadata"`
		Files    []struct {
			Name   string `json:"name"`
			Size   any    `json:"size"`
			Format string `json:"format"`
			Source string `json:"source"`
			MD5    string `json:"md5"`
			SHA1   string `json:"sha1"`
		} `json:"files"`
		Server string `json:"server"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("bad item json: %w", err)
	}
	it := &Item{Identifier: id, Metadata: raw.Metadata, Server: raw.Server}
	if t, ok := raw.Metadata["title"].(string); ok {
		it.Title = t
	}
	if t, ok := raw.Metadata["mediatype"].(string); ok {
		it.Mediatype = t
	}
	for _, f := range raw.Files {
		it.Files = append(it.Files, File{
			Name: f.Name, Format: f.Format, Source: f.Source, MD5: f.MD5, SHA1: f.SHA1,
		})
	}
	// parse size strings
	for i := range it.Files {
		if s, ok := raw.Files[i].Size.(string); ok {
			var n int64
			fmt.Sscanf(s, "%d", &n)
			it.Files[i].Size = n
		} else if f, ok := raw.Files[i].Size.(float64); ok {
			it.Files[i].Size = int64(f)
		} else {
			it.Files[i].Size = 0
		}
	}
	return it, nil
}

// DownloadURL returns the redirecting download URL for a file.
func (c *Client) DownloadURL(id, name string) string {
	return Base + "/download/" + url.PathEscape(id) + "/" + url.PathEscape(name)
}

func (c *Client) get(ctx context.Context, u string) ([]byte, error) {
	if c.Fetch != nil {
		return c.Fetch(ctx, u)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "musget/1.0")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("archive.org %s: HTTP %d", u, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64<<20))
}

// ExtractNode parses the redirected download host from a HEAD/GET, e.g.
// dn801200.us.archive.org, so callers can hit it directly.
func ExtractNode(resp *http.Response) string {
	loc := resp.Header.Get("Location")
	if i := strings.Index(loc, "//"); i >= 0 {
		rest := loc[i+2:]
		if j := strings.IndexAny(rest, "/"); j >= 0 {
			return rest[:j]
		}
	}
	return ""
}
