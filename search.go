package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"musget/pkg/archivex"
	"musget/pkg/gallica"
	"musget/pkg/netx"
)

type commonOpts struct {
	proxy string
	src   string
}

func parseCommon(fs *flag.FlagSet, args []string) (commonOpts, error) {
	fs.StringVar(&proxyFlag, "proxy", "", "proxy URL (auto-detect if empty)")
	fs.StringVar(&srcFlag, "source", "auto", "source: archive|gallica|auto")
	if err := fs.Parse(args); err != nil {
		return commonOpts{}, err
	}
	return commonOpts{proxy: proxyFlag, src: srcFlag}, nil
}

var (
	proxyFlag string
	srcFlag   string
)

// resolveProxy picks a working proxy for host, using flag > env > smart-proxy.
func resolveProxy(host string) string {
	if proxyFlag != "" {
		return proxyFlag
	}
	if p := netx.EnvProxy(); p != "" {
		return p
	}
	if p, err := netx.DetectProxy(host, 10*time.Second); err == nil {
		return p
	}
	return ""
}

func cmdSearch(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("search needs a query")
	}
	query := args[0]
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	limit := fs.Int("limit", 20, "max results")
	src := fs.String("source", "auto", "archive|gallica")
	fs.StringVar(&proxyFlag, "proxy", "", "proxy URL (or cors:<relay>)")
	fs.StringVar(&relayFlag, "relay", "", "force CORS relay (URL or cors.sh/eu.org)")
	fs.Parse(args[1:])
	ctx, cancel := context.WithTimeout(rootCtx, 120*time.Second)
	defer cancel()

	switch *src {
	case "auto", "archive":
		ac, m, bases, speeds, err := pickArchiveClient(ctx)
		if err != nil {
			return err
		}
		if m == modeCORS {
			ac.Fetch = curlFetch(bases, speeds)
		}
		fmt.Fprintf(os.Stderr, "[archive.org via %s]\n", m)
		results, err := ac.Search(ctx, query, *limit)
		if err != nil {
			if *src == "archive" {
				return err
			}
			fmt.Fprintln(os.Stderr, "archive.org search failed:", err)
		} else {
			printArchiveResults(results)
			if *src == "archive" {
				return nil
			}
		}
		// fall through to gallica when auto
		if *src == "auto" {
			gc := gallica.NewClient(proxyFlag)
			hits, err := gc.Search(ctx, cqlFor(query), *limit)
			if err != nil {
				return fmt.Errorf("gallica: %w", err)
			}
			printGallicaHits(hits)
		}
	case "gallica":
		gc := gallica.NewClient(proxyFlag)
		hits, err := gc.Search(ctx, cqlFor(query), *limit)
		if err != nil {
			return err
		}
		printGallicaHits(hits)
	default:
		return fmt.Errorf("unknown source %q", *src)
	}
	return nil
}

func cqlFor(q string) string {
	q = strings.TrimSpace(q)
	// split on spaces into title-all terms
	terms := strings.Fields(q)
	for i := range terms {
		terms[i] = `dc.title all "` + strings.Trim(terms[i], `"'`) + `"`
	}
	return strings.Join(terms, " and ")
}

func printArchiveResults(rs []archivex.Result) {
	fmt.Printf("archive.org — %d results:\n", len(rs))
	for _, r := range rs {
		restricted := ""
		if r.AccessRestricted == "true" {
			restricted = " [RESTRICTED]"
		}
		desc := strings.Join(strings.Fields(r.Str(r.Description)), " ")
		if len(desc) > 70 {
			desc = desc[:70]
		}
		fmt.Printf("  %-52s %s%s\n", truncStr(r.Identifier, 52), truncStr(r.Title, 60), restricted)
		if desc != "" {
			fmt.Printf("    %s\n", desc)
		}
	}
}

func printGallicaHits(hits []gallica.Hit) {
	fmt.Printf("gallica — %d results:\n", len(hits))
	for _, h := range hits {
		t := strings.Join(strings.Fields(h.Title), " ")
		if len(t) > 90 {
			t = t[:90]
		}
		fmt.Printf("  %-18s %s\n", h.Ark, t)
	}
}

func cmdInfo(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("info needs an identifier")
	}
	id := args[0]
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	proxy := fs.String("proxy", "", "proxy URL")
	fs.Parse(args[1:])
	p := *proxy
	if p == "" {
		p = resolveProxy("archive.org")
	}
	ctx, cancel := context.WithTimeout(rootCtx, 60*time.Second)
	defer cancel()
	ac, m, bases, speeds, err := pickArchiveClient(ctx)
	if err != nil {
		return err
	}
	if m == modeCORS {
		ac.Fetch = curlFetch(bases, speeds)
	}
	it, err := ac.Item(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("%s — %s (%s)\n", it.Identifier, it.Title, it.Mediatype)
	total := int64(0)
	for _, f := range it.Files {
		total += f.Size
	}
	fmt.Printf("files: %d, total %s\n", len(it.Files), humanSize(total))
	for _, f := range it.Files {
		if f.Format == "Metadata" || strings.Contains(strings.ToLower(f.Name), "sample") {
			continue
		}
		fmt.Printf("  %-12s %-70s %s\n", f.Format, truncStr(f.Name, 70), humanSize(f.Size))
	}
	return nil
}

func humanSize(n int64) string {
	switch {
	case n >= 1<<30:
		return strconv.FormatFloat(float64(n)/1e9, 'f', 2, 64) + " GB"
	case n >= 1<<20:
		return strconv.FormatFloat(float64(n)/1e6, 'f', 1, 64) + " MB"
	default:
		return strconv.FormatFloat(float64(n)/1e3, 'f', 0, 64) + " KB"
	}
}

func truncStr(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
