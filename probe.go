package main

import (
	"context"
	"fmt"
	"time"

	"musget/pkg/corsx"
	"musget/pkg/netx"
)

// probeResult is the connectivity matrix entry.
type probeResult struct {
	host  string
	label string
	code  int
	ok    bool
	via   string
}

func cmdProbe(args []string) error {
	fmt.Println("musget probe — connectivity matrix")
	fmt.Println()

	hosts := []struct{ host, label string }{
		{"archive.org", "archive.org (music items)"},
		{"gallica.bnf.fr", "gallica.bnf.fr (BnF scores)"},
		{"imslp.org", "imslp.org (scores, bot-gated)"},
		{"web.archive.org", "web.archive.org (wayback)"},
	}
	var results []probeResult
	for _, h := range hosts {
		// direct
		c := netx.NewClient("", 12*time.Second)
		code, ok := netx.Reachable(c, "https://"+h.host, 12*time.Second)
		if ok && code < 500 {
			results = append(results, probeResult{h.host, h.label, code, true, "direct"})
			continue
		}
		// via default proxy
		for _, p := range netx.DefaultProxyCandidates {
			pc := netx.NewClient(p, 12*time.Second)
			code, ok := netx.Reachable(pc, "https://"+h.host, 12*time.Second)
			if ok && code < 500 {
				results = append(results, probeResult{h.host, h.label, code, true, p})
				break
			}
		}
		if len(results) == 0 || results[len(results)-1].host != h.host {
			results = append(results, probeResult{h.host, h.label, 0, false, "-"})
		}
	}
	for _, r := range results {
		status := "OK"
		if !r.ok {
			status = "UNREACHABLE"
		}
		fmt.Printf("  %-22s %-10s via %s (HTTP %d)\n", r.label, status, r.via, r.code)
	}
	fmt.Println()

	// CORS relays: probe each with a 1MB ranged request, report speed and
	// the chosen (fastest) one.
	ctx, cancel := context.WithTimeout(rootCtx, 30*time.Second)
	defer cancel()
	probes := probeRelays(ctx, corsx.Bases)
	fmt.Println("CORS relays (curl probe, 1MB ranged, 5s timeout):")
	if len(probes) == 0 {
		fmt.Println("  (none reachable)")
	} else {
		for _, p := range probes {
			status := "OK"
			if !p.ok {
				status = "UNREACHABLE"
			}
			fmt.Printf("  %-24s %-10s %6.2f MB/s\n", p.base, status, p.speed)
		}
		if probes[0].ok {
			fmt.Printf("  chosen: %s (fastest)\n", probes[0].base)
		}
	}
	fmt.Println()
	fmt.Println("Recommended:")
	fmt.Println("  archive.org  -> use --proxy http://127.0.0.1:8888 (smart-proxy) on this box")
	fmt.Println("  gallica      -> direct; downloads solve altcha PoW automatically")
	fmt.Println("  imslp.org    -> metadata only (file downloads are captcha-gated)")
	return nil
}
