#!/usr/bin/env python3
"""Validate alembic revision chain has no broken references.

Catches the common error where a merge migration references a parent
revision ID that doesn't match the actual `revision` variable inside
the parent file (e.g. filename says 20260330_merge_cu_cols_and_exp_adv
but revision inside says 20260330_merge_cu_exp_adv).

Run this before deploy to prevent migration failures in production.
"""
import re
import sys
from pathlib import Path


def validate():
    versions_dir = Path("alembic/versions")
    if not versions_dir.exists():
        print("ERROR: alembic/versions/ directory not found")
        sys.exit(1)

    revision_ids = set()
    file_revisions = {}  # {revision_id: filename}
    references = {}  # {filename: [referenced_revision_ids]}
    duplicate_revisions = []

    for f in sorted(versions_dir.glob("*.py")):
        if f.name == "__pycache__":
            continue
        content = f.read_text()

        # Extract revision = "..."
        rev_match = re.search(r'^revision\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
        if not rev_match:
            continue

        rev_id = rev_match.group(1)
        if rev_id in revision_ids:
            duplicate_revisions.append((rev_id, file_revisions[rev_id], f.name))
        revision_ids.add(rev_id)
        file_revisions[rev_id] = f.name

        # Extract down_revision (string, tuple, or None)
        down_match = re.search(r'^down_revision\s*=\s*(.+)$', content, re.MULTILINE)
        if not down_match:
            continue
        val = down_match.group(1).strip()
        if val == "None" or val == "none":
            continue
        refs = re.findall(r'["\']([^"\']+)["\']', val)
        if refs:
            references[f.name] = refs

    errors = []

    # Check for broken references
    for fname, refs in references.items():
        for ref in refs:
            if ref not in revision_ids:
                errors.append(
                    f"{fname}: down_revision references '{ref}' "
                    f"which does not exist as a revision ID in any file"
                )

    # Check for duplicate revision IDs
    for rev_id, first_file, second_file in duplicate_revisions:
        errors.append(
            f"Duplicate revision ID '{rev_id}' found in both "
            f"{first_file} and {second_file}"
        )

    # Report
    if errors:
        print(f"ALEMBIC CHAIN ERRORS ({len(errors)}):")
        for e in errors:
            print(f"  ✗ {e}")
        print()
        print("Fix: ensure every down_revision references an exact revision ID")
        print("     that exists inside another migration file's `revision = ...`")
        sys.exit(1)
    else:
        print(f"✓ All {len(revision_ids)} alembic revisions have valid references")
        sys.exit(0)


if __name__ == "__main__":
    validate()
