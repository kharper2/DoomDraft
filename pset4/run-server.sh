#!/usr/bin/env bash
# Wrapper: launch pt-collab-server with stderr/stdout timestamped (μs precision)
# and tee'd to pset4/logs/server-<local-date>.log. Use this instead of running the
# binary directly so every line is wall-clock-correlated and saved for later.
#
# Usage:
#   ./run-server.sh --port 8080
#   ./run-server.sh --port 8080 --replicas 3 --doc main

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOGS_DIR"

TS="$(date +%Y%m%d-%H%M%S)"
LOG="$LOGS_DIR/server-$TS.log"

echo "==> log file: $LOG"
echo "==> command : $SCRIPT_DIR/build/pt-collab-server $*"
echo "==> tail with: tail -f $LOG"
echo

# Time::HiRes ships with the macOS system Perl, no extra install needed.
# Each output line is prefixed with [HH:MM:SS.uuuuuu] in local time.
exec "$SCRIPT_DIR/build/pt-collab-server" "$@" 2>&1 \
  | perl -MTime::HiRes=gettimeofday -ne '
      my ($s, $u) = gettimeofday;
      my @t = localtime($s);
      printf "[%02d:%02d:%02d.%06d] %s", $t[2], $t[1], $t[0], $u, $_;
      STDOUT->flush;
    ' \
  | tee "$LOG"
