# musget/pkg — Public API for videocrawl

The module path stays **`musget`**; the packages were moved from `internal/`
to `pkg/` precisely so other modules (e.g. videocrawl) can import them. In
videocrawl's `go.mod`:

```
require musget v0.0.0
replace musget => ../musget   // or wherever musget is checked out
```

All packages below carry Go doc comments; `go doc musget/pkg/gallica` etc.
works after the replace.

---

## 1. gallica — `import "musget/pkg/gallica"`

Base URL: `const Base = "https://gallica.bnf.fr"`

```go
func NewClient(proxy string) *Client          // proxy "" = direct; else HTTP proxy URL
type Client struct {
    HTTP     *http.Client   // custom transport allowed; session cookie jar shared
    MaxTries int            // default 8 (retries w/ exponential backoff)
}
func (c *Client) Search(ctx context.Context, query string, limit int) ([]Hit, error)
type Hit struct { Ark, Title, Type, Pages string }

// Download is the one call videocrawl needs for "solve altcha + fetch a PDF":
func (c *Client) Download(ctx context.Context, ark, dest string) error
```

- `Download` fetches `https://gallica.bnf.fr/ark:/12148/<ark>.pdf` directly.
  **Altcha is solved automatically, once per session, and the verified cookie
  is kept in `Client.HTTP`'s jar** — there is no separate exported solve
  function; `Download` handles the whole flow (seed session → GET
  `/services/engine/search/altcha/challenge` → brute-force
  `SHA-256(salt+counter) == challenge` (≤100000) → POST
  `/services/engine/search/altcha/verify` → re-fetch PDF). Call it repeatedly
  on the same `*Client` for batch downloads; only the first non-PDF response
  triggers a solve.
- Uses its own `http.Transport` (Go native TLS is fine for gallica.bnf.fr),
  a Chrome UA header, a 1200 ms min-interval throttle, 512 MiB read cap and
  internal retry/backoff on 429. `MaxTries` is adjustable.

---

## 2. archivex — `import "musget/pkg/archivex"`

Base URL: `const Base = "https://archive.org"`. Only stdlib + `musget/pkg/corsx`.

```go
func NewDirectClient() *Client                       // direct archive.org
func NewProxyClient(proxy string) *Client            // via HTTP proxy
func NewCORSClient(base string) *Client              // via CORS relay (corsx)

type Client struct {
    HTTP  *http.Client
    Fetch func(ctx context.Context, u string) ([]byte, error) // optional override; used if non-nil
}

// item metadata + files:
func (c *Client) Search(ctx context.Context, query string, limit int) ([]Result, error)
func (c *Client) Item(ctx context.Context, id string) (*Item, error)   // GET /metadata/<id>
type Result struct { Identifier, Title, Mediatype string; Description, Year, Creator any; Collection []string; DownloadCount int; AccessRestricted string }
func (r *Result) Str(f any) string                  // normalize scalar-or-array metadata to string
type Item struct {
    Identifier, Title, Mediatype string
    Metadata map[string]any
    Files    []File
    Server   string
}
type File struct { Name string; Size int64; Format, Source, MD5, SHA1, Length string }

// file selection + download URL (redirects to dnNxxx.us.archive.org):
func (c *Client) DownloadURL(id, name string) string
func ExtractNode(resp *http.Response) string        // parse redirected download host from Location
```

- **Get metadata**: `Item(ctx, id)` → `Item.Metadata` (raw map) + `Item.Files`
  (name/size/format/md5/sha1). **List/select files**: iterate `Item.Files`,
  filter on `Format` (e.g. `"VBR MP3"`, `"64Kbps MP3"`, `"Ogg Vorbis"`,
  `"JPEG"`) or `Name` glob, then build the URL with `DownloadURL(id, name)`
  and download with any HTTP client (follow redirects to the dn node).
- `Client.Fetch` (if set) takes precedence over `HTTP` for all API calls —
  musget uses it to route through curl when the relay rejects Go's TLS
  fingerprint (see §4).

---

## 3. corsx — `import "musget/pkg/corsx"`

Relay transport that re-wraps redirects through the relay automatically.

```go
// Relay chain order — tried in this order:
var Bases = []string{
    "https://proxy.cors.sh",   // primary
    "https://cors.eu.org",     // fallback
}
var ShortNames = map[string]string{"cors.sh": "https://proxy.cors.sh", "eu.org": "https://cors.eu.org"}
func CanonicalBase(s string) string          // "" if unrecognized
func NewTransport(base string, inner http.RoundTripper) *Transport  // inner nil => default clone
func Check(ctx context.Context, timeout time.Duration) string       // first base that reaches archive.org, or ""
type Transport struct { Base string; Inner http.RoundTripper; Timeout time.Duration }
```

- **Relay chain order** (how musget's CLI composes it, see `transport.go`):
  1. direct `https://archive.org` (reachability probe, 8 s);
  2. CORS relays — all of `corsx.Bases` probed **concurrently** with a ~1 MB
     ranged curl request, healthy ones sorted **fastest-first**, rotated per
     job with per-relay cooldowns and hard-fail marking;
  3. HTTP proxy (env `HTTP(S)_PROXY` / WARP smart-proxy detect) as last resort.
  `--relay cors.sh|eu.org|URL` forces a single relay. `corsx.Transport`'s own
  hop budget: **up to 4 redirect hops** (`original → relay → dn node → relay`),
  each 3xx re-wrapped through the relay; relay CORS headers are stripped.
- **TLS note (important)**: proxy.cors.sh / cors.eu.org are fronted by
  Cloudflare and return **403 for Go's native TLS fingerprint**. musget never
  uses `NewCORSClient`'s stock inner transport in production — the CLI probes
  relays with `curl` and streams downloads through `musget/pkg/utlsx`'s
  Chrome-fingerprint transport (§4). videocrawl should do the same:
  `corsx.NewTransport(base, utlsx.Transport())` for relayed requests, or the
  curl approach for API calls.

---

## 4. utlsx (relay TLS) — `import "musget/pkg/utlsx"`

```go
func DialTLSContext(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error)
//   — matches http2.Transport.DialTLSContext; Chrome 120 ClientHello (uTLS)
func Transport() http.RoundTripper
//   — http2.Transport with DialTLSContext set; use as the inner RoundTripper
//     for corsx.NewTransport when talking to the Cloudflare-fronted relays
```

Videocrawl needs this **only if it routes through the CORS relays** (it is
not needed for gallica or direct archive.org). musget's engine additionally
uses a `hybridRT` that falls back to h1 plain-TLS when the server doesn't
negotiate HTTP/2 (see `pkg/engine/curl.go`); simplest correct usage for
videocrawl is `&http.Client{Transport: corsx.NewTransport(base, utlsx.Transport())}`.

---

## 5. Dependencies videocrawl must add

Packages `gallica`, `archivex`, `corsx` import **stdlib only** (archivex →
corsx). `utlsx` pulls in the external deps. After adding the
`replace musget => ../musget`, run `go mod tidy` in videocrawl — it must
resolve the same versions musget's `go.mod`/`go.sum` pin (go.sum entries are
already present in musget; versions used today):

| module | version |
|---|---|
| `github.com/refraction-networking/utls` | v1.8.2 |
| `golang.org/x/net` | v0.58.0 (for `http2`) |
| `golang.org/x/crypto` | v0.55.0 |
| `golang.org/x/sys` | v0.47.0 |
| `golang.org/x/text` | v0.41.0 |
| `github.com/andybalholm/brotli` | v1.0.6 (indirect, via utls) |
| `github.com/klauspost/compress` | v1.17.4 (indirect, via utls) |

All are `// indirect` in musget's `go.mod`; `go mod tidy` in videocrawl will
pull exactly what `utlsx` needs. musget's `go.mod` declares `go 1.26.5` —
videocrawl's toolchain must be ≥ that.
