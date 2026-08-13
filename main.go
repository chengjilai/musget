package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const usage = `musget — find & download music (archive.org, Gallica)

Usage:
  musget probe                     check which sources are reachable (direct/proxy)
  musget search <query> [flags]    search for music
  musget info <identifier>         show item metadata + files
  musget get <identifier> [flags]  download an item's files (parallel, resumable)

Flags:
  --source archive|gallica|auto   source to search (default auto)
  --limit N                       max search results (default 20)
  --kind audio|score|all          file types to download (default all)
  --file GLOB                     only files matching GLOB (repeatable)
  --out DIR                       output directory (default ~/Music)
  --jobs N                        parallel downloads (default 8)
  --segments N                    split large files into N parallel ranges (default 0=off)
  --segment-min MB                min size to use segmented download (default 25)
  --verify                        verify sha1/md5 after download (default on)
  --proxy URL                     proxy override (auto-detect if omitted)
  --relay BASE                    force CORS relay (URL or cors.sh/eu.org)
  --install                       organize under ~/Music/<Identifier>/
  post-loop                       PD-gated auto download+repost loop (law+dedup)
  post-seed ID[:Title] ...        queue candidates
  post-status                     show the post queue
  -q, --quiet                     less output
`

// rootCtx is canceled by SIGINT/SIGTERM so long-running commands (downloads,
// probes) abort promptly: the engine's ctx.Done() paths and
// exec.CommandContext kill curl children instead of leaving orphan curl
// processes writing into .part files.
var rootCtx, rootCancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

func main() {
	defer rootCancel()
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "probe":
		err = cmdProbe(os.Args[2:])
	case "search":
		err = cmdSearch(os.Args[2:])
	case "info":
		err = cmdInfo(os.Args[2:])
	case "get":
		err = cmdGet(os.Args[2:])
	case "post-loop":
		err = cmdPostLoop(os.Args[2:])
	case "post-status":
		err = cmdPostStatus(os.Args[2:])
	case "post-seed":
		err = cmdPostSeed(os.Args[2:])
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
