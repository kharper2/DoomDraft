#!/usr/bin/env bash
# collab-bench.sh — failure testing campaign for the collaborative editor
# Run from pset4/: bash collab-bench.sh

set -e
BIN=./build/pt-collab

echo "=== pt-collab --test (doc_state + fixed-seed failure convergence) ==="
$BIN --test

echo ""
echo "=== Basic convergence (-R 100) ==="
$BIN -R 100

echo ""
echo "=== Failover (-f failover -R 50) ==="
$BIN -f failover -R 50

echo ""
echo "=== Recovery (-f recover -R 50) ==="
$BIN -f recover -R 50

echo ""
echo "=== Split brain (-f split -R 50) ==="
$BIN -f split -R 50

echo ""
echo "=== Unstable (-f unstable -R 30) ==="
$BIN -f unstable -R 30

echo ""
echo "=== Torture (-f torture -R 20) ==="
$BIN -f torture -R 20

echo ""
echo "=== High loss (-l 0.15 -R 100) ==="
$BIN -l 0.15 -R 100

echo ""
echo "All tests passed."
