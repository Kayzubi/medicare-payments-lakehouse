#!/usr/bin/env bash
set -euo pipefail   # stop on any error, unset variable or failed pipe

URL="https://data.cms.gov/sites/default/files/2026-05/0ae165f4-eb44-495d-8cac-67f4571b6b83/MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv"
OUT_DIR="data/bronze/partd"
OUT="$OUT_DIR/partd_prescriber_drug_2024.csv"
MANIFEST="$OUT_DIR/_manifest.txt"

mkdir -p "$OUT_DIR"

START=$(date +%s)
# -L follow redirects, -C - resume, --retry for flaky connections, -f fail on HTTP errors
curl -L -f -C - --retry 5 --retry-delay 10 --progress-bar -o "$OUT" "$URL"
END=$(date +%s)

SIZE=$(ls -lh "$OUT" | awk '{print $5}')
SHA=$(shasum -a 256 "$OUT" | awk '{print $1}')
ROWS=$(($(wc -l < "$OUT") - 1))   # minus header

cat > "$MANIFEST" <<EOF
source_url: $URL
downloaded_at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
size: $SIZE
sha256: $SHA
line_count_minus_header: $ROWS
download_seconds: $((END - START))
EOF

cat "$MANIFEST"
head -n 2 "$OUT"