"""Guard against duplicate Alembic revision IDs.

Scans every migration file under ``alembic/versions/`` and fails if two or
more files share the same ``revision`` identifier.
"""

from __future__ import annotations

import pathlib
import re
from collections import Counter

VERSIONS_DIR = pathlib.Path(__file__).resolve().parents[1] / "alembic" / "versions"
_REVISION_RE = re.compile(r'^revision\s*=\s*["\']([a-f0-9]+)["\']', re.MULTILINE)


def _collect_revisions() -> list[tuple[str, str]]:
    """Return a list of (revision_id, filename) pairs."""
    results: list[tuple[str, str]] = []
    for path in sorted(VERSIONS_DIR.glob("*.py")):
        text = path.read_text()
        match = _REVISION_RE.search(text)
        if match:
            results.append((match.group(1), path.name))
    return results


def test_no_duplicate_revision_ids() -> None:
    revisions = _collect_revisions()
    assert revisions, "No migration files found – check VERSIONS_DIR path"

    counts = Counter(rev_id for rev_id, _ in revisions)
    duplicates = {
        rev_id: [fname for rid, fname in revisions if rid == rev_id]
        for rev_id, count in counts.items()
        if count > 1
    }
    assert not duplicates, (
        f"Duplicate Alembic revision IDs detected:\n"
        + "\n".join(
            f"  {rev_id}: {', '.join(files)}"
            for rev_id, files in duplicates.items()
        )
    )
