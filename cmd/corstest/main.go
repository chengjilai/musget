package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"musget/internal/corsx"
	"musget/internal/utlsx"
)

func try(name string, tr http.RoundTripper, hdrs map[string]string) {
	c := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://archive.org/metadata/faure-nocturnes-1-8", nil)
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		fmt.Println(name, "ERR:", err)
		return
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 60))
	fmt.Println(name, "status:", resp.Status, "|", string(b)[:40])
}

func main() {
	// exactly curl's headers
	try("utls+curl-hdrs", corsx.NewTransport("https://proxy.cors.sh", utlsx.Transport()),
		map[string]string{"User-Agent": "curl/8.21.0", "Accept": "*/*"})
	// curl headers without Accept
	try("utls+ua-only", corsx.NewTransport("https://proxy.cors.sh", utlsx.Transport()),
		map[string]string{"User-Agent": "curl/8.21.0"})
	// no custom headers at all (Go defaults)
	try("utls+go-defaults", corsx.NewTransport("https://proxy.cors.sh", utlsx.Transport()), nil)
}
