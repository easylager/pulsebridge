#!/usr/bin/env python3
"""Export the gateway's OpenAPI document to contracts/openapi.json.

A spec that only exists at runtime cannot be reviewed, diffed or consumed by a
client generator. Committing it makes every change to the public API show up in
the pull request that causes it, and CI re-runs this script to fail on drift.

Usage:
    uv run --project services/gateway python scripts/export_openapi.py [--check]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = REPO_ROOT / "contracts" / "openapi.json"

sys.path.insert(0, str(REPO_ROOT / "services" / "gateway" / "src"))

from pulsebridge_gateway.api.main import create_app  # noqa: E402


def render() -> str:
    spec = create_app().openapi()
    return json.dumps(spec, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the committed spec is stale instead of rewriting it",
    )
    args = parser.parse_args()

    rendered = render()

    if args.check:
        current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if current != rendered:
            print(
                f"{OUTPUT.relative_to(REPO_ROOT)} is out of date.\n"
                "Run: make openapi",
                file=sys.stderr,
            )
            return 1
        print(f"{OUTPUT.relative_to(REPO_ROOT)} is up to date")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
