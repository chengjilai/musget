#!/usr/bin/env bash
# benchmark: download the Vlaeva album mp3s (~110MB) and report wall time
set -e
BIN=${BIN:-./musget}
DIR=${1:-/tmp/bench}
JOBS=${JOBS:-8}
SEGS=${SEGS:-0}
rm -rf "$DIR"
S=$(date +%s.%N)
ARGS=(get nadejda-vlaeva-bortkiewicz-piano-sonata-no.-2-other-works-2016-24-96 --kind audio --file "*.mp3" --out "$DIR" --jobs "$JOBS" -q)
[ "$SEGS" != "0" ] && ARGS+=(--segments "$SEGS")
"$BIN" "${ARGS[@]}" 2>&1 | tail -1
E=$(date +%s.%N)
echo "WALL: $(echo "$E $S" | awk '{printf "%.1fs", $1-$2}')"
