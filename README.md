# musget — find & download music fast (archive.org, Gallica)

Go CLI (stdlib + uTLS for the relay probe). Built for mainland-China networks:
auto-detects the fastest reachable path per machine and machine state.

## Quick start

```sh
go build -o musget .
./musget probe                    # connectivity matrix + relay speeds
./musget search "grieg lyric pieces" --limit 10
./musget info nadejda-vlaeva-bortkiewicz-piano-sonata-no.-2-other-works-2016-24-96
./musget get <identifier> --kind audio --file "*.mp3" --out ~/Music  --jobs 16
./musget get bpt6k1159259w        # Gallica ark → PDF (altcha solved automatically)
./musget get <id> --install       # organize under ~/Music/<Identifier>/
```

## Transport auto-selection (the speed story)

archive.org is GFW-blocked on this box AND on `lab` (SJTU). The tool picks, in order:

1. **direct** (if reachable)
2. **CORS relay** (`proxy.cors.sh`, `cors.eu.org`) — Cloudflare workers that fetch
   archive.org from CN at **6–8 MB/s** (measured; 20× faster than WARP). Transfers
   shell out to `curl` because the relays' Cloudflare rejects Go's TLS fingerprint
   (verified: uTLS+Chrome ClientHello still 403s — JA4 includes h2 SETTINGS).
   Segments of one file are batched into a single `curl -Z --parallel` invocation
   (one TLS handshake + one redirect-resolve per job).
3. **HTTP proxy** (WARP smart-proxy `127.0.0.1:8888`, ~1 MB/s shared cap) — fallback.

Relay health: startup speed probe (1 MB ranged request, fastest wins), weighted
pick (fewest recent failures), soft cooldown 20 s on 4xx/5xx/stalls, hard 60 s on
connect failures, WARP fallback when all relays are down, adaptive worker pool
(AIMD-lite: grows on rising throughput, shrinks on sustained collapse).

Gallica (BnF) is reachable direct from both machines; PDF downloads solve the
altcha proof-of-work (`SHA-256(salt+counter)==challenge`) once per session and
reuse the verified cookie. 429s are retried with exponential backoff.

## Performance (110 MB album, 26 files, median)

| path | wall time | MB/s |
|---|---|---|
| WARP proxy (old) | ~188 s | 1.15 |
| CORS relay, healthy window | **13–16 s** | **6.5–7.6** |
| CORS relay, rate-limited window | 40–60 s | 2–2.5 |

Big file (270 MB FLAC): ~18–23 s when relays are healthy; batch path eliminates
the 50 s+ rate-limit storms seen with per-segment curl processes.

## Features

- parallel worker pool (`--jobs`, auto default 16 on relay path)
- segmented downloads (`--segments N`, auto buckets: 8–40 MB→2, >40→4, >120→6)
- resume (curl `-C` / byte offsets), checksum verify (sha1/md5 from metadata)
- relay rotation & failover, WARP fallback, adaptive concurrency
- stall detection (`--speed-limit`), exponential retry backoff
- SIGINT/SIGTERM kills curl children (no orphaned .part writers)
- Gallica: SRU search (filters out sound recordings), altcha solver, session reuse

## Layout

```
main.go get.go search.go probe.go transport.go
internal/archivex   archive.org search/metadata client
internal/gallica    SRU search + altcha PoW downloader
internal/engine     worker pool, segmented/parallel curl batches, resume, verify
internal/corsx      CORS-relay base list + probe
internal/utlsx      uTLS transport (probe only; relays use curl)
internal/netx       direct/proxy detection
```

Benchmark: `./bench.sh` (downloads a 26-track ~110 MB album via the relay path).
`lab` (SJTU, 28 cores): `rsync -az --delete -e ssh ~/musget/ lab:~/musget/ && ssh lab 'cd ~/musget && go build -o musget .'`.
