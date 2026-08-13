// Package utlsx provides a dialer that mimics a Chrome TLS ClientHello so
// Cloudflare-fronted relays (proxy.cors.sh, cors.eu.org) accept connections
// from Go (they 403 Go's native TLS fingerprint).
package utlsx

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
)

// DialTLSContext dials addr and performs a Chrome-like TLS handshake.
// Signature matches http2.Transport.DialTLSContext.
func DialTLSContext(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
	d := &net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	uconn := utls.UClient(conn, &utls.Config{ServerName: host, InsecureSkipVerify: false}, utls.HelloChrome_120)
	if err := uconn.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return uconn, nil
}

// Transport returns an http2-capable RoundTripper whose TLS handshakes use a
// Chrome fingerprint.
func Transport() http.RoundTripper {
	return &http2.Transport{
		DialTLSContext:            DialTLSContext,
		MaxReadFrameSize:          1 << 20,
		MaxDecoderHeaderTableSize: 1 << 20,
	}
}
