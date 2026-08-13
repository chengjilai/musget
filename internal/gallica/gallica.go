// Package gallica searches Gallica (BnF) and downloads PDFs by solving its
// altcha proof-of-work challenge (SHA-256(salt+counter)==challenge) once per
// session, then reusing the verified cookie for batch downloads.
package gallica

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const Base = "https://gallica.bnf.fr"

// Client holds a session (cookie jar) shared across downloads.
type Client struct {
	HTTP     *http.Client
	MaxTries int
	verified bool
}

func NewClient(proxy string) *Client {
	jar, _ := cookiejar.New(nil)
	var tr *http.Transport
	if proxy != "" {
		tr = &http.Transport{ForceAttemptHTTP2: true}
		if u, err := url.Parse(proxy); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	} else {
		tr = &http.Transport{ForceAttemptHTTP2: true, MaxIdleConns: 64, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second}
	}
	return &Client{
		HTTP:     &http.Client{Transport: tr, Jar: jar, Timeout: 300 * time.Second},
		MaxTries: 5,
	}
}

// Hit is one search result.
type Hit struct {
	Ark   string
	Title string
	Type  string
	Pages string
}

var (
	reRecord = regexp.MustCompile(`(?s)<srw:record>(.*?)</srw:record>`)
	reField  = regexp.MustCompile(`(?s)<(?:dc|dcx):([a-zA-Z]+)[^>]*>(.*?)</(?:dc|dcx):[a-zA-Z]+>`)
	reArk    = regexp.MustCompile(`ark:/12148/([a-z0-9]+)`)
)

// Search queries the SRU endpoint. query is a CQL string over dc fields.
func (c *Client) Search(ctx context.Context, query string, limit int) ([]Hit, error) {
	if limit <= 0 {
		limit = 20
	}
	q := url.Values{}
	q.Set("operation", "searchRetrieve")
	q.Set("version", "1.2")
	q.Set("query", query+` and not dc.type any "sound" and not dc.type any "video"`)
	q.Set("maximumRecords", fmt.Sprint(limit))
	body, err := c.get(ctx, Base+"/SRU?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var hits []Hit
	for _, rec := range reRecord.FindAllString(string(body), -1) {
		fields := map[string]string{}
		for _, m := range reField.FindAllStringSubmatch(rec, -1) {
			fields[m[1]] = strings.TrimSpace(stripTags(m[2]))
		}
		ark := ""
		if m := reArk.FindStringSubmatch(fields["identifier"]); m != nil {
			ark = m[1]
		} else if m := reArk.FindStringSubmatch(rec); m != nil {
			ark = m[1]
		}
		if ark == "" {
			continue
		}
		hits = append(hits, Hit{Ark: ark, Title: fields["title"], Type: fields["type"], Pages: fields["format"]})
	}
	return hits, nil
}

func stripTags(s string) string {
	var b strings.Builder
	in := false
	for _, r := range s {
		switch {
		case r == '<':
			in = true
		case r == '>':
			in = false
		case !in:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Download fetches the full PDF for an ark into dest, solving altcha as needed.
func (c *Client) Download(ctx context.Context, ark, dest string) error {
	u := fmt.Sprintf("%s/ark:/12148/%s.pdf", Base, ark)
	data, err := c.get(ctx, u, nil)
	if err != nil {
		return err
	}
	if !isPDF(data) {
		if !c.verified {
			if err := c.solveAltcha(ctx, u); err != nil {
				return fmt.Errorf("altcha: %w", err)
			}
			c.verified = true
		}
		data, err = c.get(ctx, u, nil)
		if err != nil {
			return err
		}
		if !isPDF(data) {
			return fmt.Errorf("still not a PDF after altcha (%d bytes)", len(data))
		}
	}
	return os.WriteFile(dest, data, 0o644)
}

func isPDF(b []byte) bool {
	return len(b) >= 4 && string(b[:4]) == "%PDF"
}

func (c *Client) get(ctx context.Context, u string, headers map[string]string) ([]byte, error) {
	var lastErr error
	for try := 0; try < c.MaxTries; try++ {
		if try > 0 {
			// exponential backoff: 3s, 6s, 12s, 24s, 48s ...
			backoff := time.Duration(3) * time.Second
			for i := 0; i < try; i++ {
				backoff *= 2
			}
			if backoff > 3*time.Minute {
				backoff = 3 * time.Minute
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		// polite min-interval throttle (gallica rate-limits aggressively)
		if try == 0 {
			time.Sleep(1200 * time.Millisecond)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			lastErr = fmt.Errorf("429 Too Many Requests")
			continue // backoff
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("gallica HTTP %d", resp.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 512<<20))
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return data, nil
	}
	return nil, lastErr
}

// solveAltcha performs the full PoW flow and keeps the session cookie.
func (c *Client) solveAltcha(ctx context.Context, referer string) error {
	// seed a session
	_, _ = c.get(ctx, referer, map[string]string{"Referer": Base + "/"})
	// fetch challenge
	body, err := c.get(ctx, Base+"/services/engine/search/altcha/challenge", map[string]string{"Referer": referer})
	if err != nil {
		return err
	}
	var ch struct {
		Algorithm string `json:"algorithm"`
		Challenge string `json:"challenge"`
		Maxnumber int    `json:"maxnumber"`
		Salt      string `json:"salt"`
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(body, &ch); err != nil {
		return fmt.Errorf("challenge json: %w", err)
	}
	if ch.Algorithm != "SHA-256" {
		return fmt.Errorf("unsupported altcha algorithm %q", ch.Algorithm)
	}
	maxN := ch.Maxnumber
	if maxN <= 0 {
		maxN = 100000
	}
	sol := -1
	start := time.Now()
	for n := 0; n <= maxN; n++ {
		sum := sha256.Sum256([]byte(ch.Salt + fmt.Sprint(n)))
		if hex.EncodeToString(sum[:]) == ch.Challenge {
			sol = n
			break
		}
		if n%50000 == 0 && time.Since(start) > 20*time.Second {
			return fmt.Errorf("altcha solve timed out at %d", n)
		}
	}
	if sol < 0 {
		return fmt.Errorf("altcha no solution in [0,%d]", maxN)
	}
	payload, _ := json.Marshal(map[string]any{
		"algorithm": ch.Algorithm,
		"challenge": ch.Challenge,
		"number":    sol,
		"salt":      ch.Salt,
		"signature": ch.Signature,
	})
	b64 := base64.StdEncoding.EncodeToString(payload)
	form := url.Values{"altchaPayload": {b64}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		Base+"/services/engine/search/altcha/verify", strings.NewReader(form))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", referer)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusFound {
		return fmt.Errorf("altcha verify HTTP %d", resp.StatusCode)
	}
	return nil
}
