#!/usr/bin/env bash
# One-shot backfill of 24 months of Black Marble VNP46A3 (Apr 2024 -> Mar 2026).
# Run after the new migration is deployed and EDL_TOKEN is set in env.
# Sequential: ~80 MB per file x 24 = ~2 GB total, ~30 min wall time.
# NOT for production cron use; the monthly Actions workflow handles ongoing
# ingest. Re-running is safe (each month pre-purges its own rows).

set -euo pipefail

if [ -z "${EDL_TOKEN:-}" ]; then
  echo "ERROR: EDL_TOKEN not set" >&2
  exit 1
fi

for ym in 2024-04 2024-05 2024-06 2024-07 2024-08 2024-09 2024-10 2024-11 2024-12 \
          2025-01 2025-02 2025-03 2025-04 2025-05 2025-06 2025-07 2025-08 2025-09 \
          2025-10 2025-11 2025-12 2026-01 2026-02 2026-03; do
  echo "=== Ingesting $ym ==="
  python -m app.ingest.black_marble_radiance --year-month "$ym"
done

echo "Backfill complete. 24 months ingested."
