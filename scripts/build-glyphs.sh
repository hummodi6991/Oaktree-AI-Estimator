#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "[build-glyphs] ERROR: docker not found; cannot generate glyphs"
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$REPO_ROOT/frontend/public/glyphs"
FONT_DIR="$REPO_ROOT/frontend/assets/fonts"
FONT_FILE="$FONT_DIR/NotoNaskhArabic-Regular.ttf"
FONT_STACK="NotoNaskhArabic-Regular"

mkdir -p "$OUT_DIR" "$FONT_DIR"

if [ ! -f "$FONT_FILE" ]; then
  curl -fsSL -o "$FONT_FILE" \
    "https://raw.githubusercontent.com/googlefonts/noto-fonts/main/hinted/ttf/NotoNaskhArabic/NotoNaskhArabic-Regular.ttf"
fi

TMP_IN="$(mktemp -d)"
trap 'rm -rf "$TMP_IN"' EXIT
mkdir -p "$TMP_IN/$FONT_STACK"
cp "$FONT_FILE" "$TMP_IN/$FONT_STACK/${FONT_STACK}.ttf"

echo "[build-glyphs] generating glyphs via jmbarbier/fontnik -> $OUT_DIR/$FONT_STACK"
docker pull jmbarbier/fontnik:latest >/dev/null
docker run --rm \
  -v "$TMP_IN:/fonts/input" \
  -v "$OUT_DIR:/fonts/output" \
  jmbarbier/fontnik:latest \
  /fonts/input /fonts/output

test -s "$OUT_DIR/$FONT_STACK/0-255.pbf"
test -s "$OUT_DIR/$FONT_STACK/1536-1791.pbf"
echo "[build-glyphs] ok"
