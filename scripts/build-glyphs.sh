#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$REPO_ROOT/frontend/public/glyphs"
FONT_DIR="$REPO_ROOT/frontend/assets/fonts"
FONT_FILE="$FONT_DIR/NotoNaskhArabic-Regular.ttf"
FONT_STACK="NotoNaskhArabic-Regular"
OUT_STACK_DIR="$OUT_DIR/$FONT_STACK"

mkdir -p "$OUT_DIR"
mkdir -p "$FONT_DIR"
mkdir -p "$OUT_STACK_DIR"

if [ ! -f "$FONT_FILE" ]; then
  curl -fsSL -o "$FONT_FILE" \
    "https://raw.githubusercontent.com/googlefonts/noto-fonts/main/hinted/ttf/NotoNaskhArabic/NotoNaskhArabic-Regular.ttf"
fi

if command -v docker >/dev/null 2>&1; then
  docker run --rm -v "$REPO_ROOT:/work" -w /work openmaptiles/fonts \
    generate --font "/work/frontend/assets/fonts/NotoNaskhArabic-Regular.ttf" \
    --name "$FONT_STACK" \
    --output "/work/frontend/public/glyphs"

  # Sanity: expected primary range must exist and be non-empty.
  test -s "$OUT_STACK_DIR/0-255.pbf"
  exit 0
fi

if [ "${CI:-}" = "true" ]; then
  echo "[build-glyphs] ERROR: docker not found in CI; cannot generate glyphs"
  exit 1
fi

echo "[build-glyphs] docker not found; skipping glyph generation (local dev)"
exit 0
