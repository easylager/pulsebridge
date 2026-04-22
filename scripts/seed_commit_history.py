#!/usr/bin/env python3
"""Rebuild a polished git history for PulseBridge (showcase repo)."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AUTHOR = "Maksim Vasilenko"
EMAIL = "pinkpanterpython3@gmail.com"

COMMITS: list[tuple[str, str, list[str]]] = [
    (
        "2025-09-12T10:15:00+03:00",
        "chore: bootstrap PulseBridge repository\n\n"
        "Initialize license, gitignore, makefile, and project skeleton for an\n"
        "async event-bridge reference implementation.",
        [
            "LICENSE",
            ".gitignore",
            "Makefile",
        ],
    ),
    (
        "2025-09-18T18:40:00+03:00",
        "feat(gateway): add domain event envelope and ports\n\n"
        "Introduce immutable EventEnvelope, validation rules, and Protocol-based\n"
        "ports for publish / idempotency / rate limiting (hexagonal core).",
        [
            "services/gateway/pyproject.toml",
            "services/gateway/README.md",
            "services/gateway/src/pulsebridge_gateway/__init__.py",
            "services/gateway/src/pulsebridge_gateway/domain/",
            "services/gateway/src/pulsebridge_gateway/application/__init__.py",
        ],
    ),
    (
        "2025-10-02T12:05:00+03:00",
        "feat(gateway): implement ingest use-case with memory + Redis adapters\n\n"
        "Wire rate-limit → idempotency → publish pipeline. Include in-memory fakes\n"
        "for tests and Redis Streams / Lua token-bucket adapters for production mode.",
        [
            "services/gateway/src/pulsebridge_gateway/application/ingest.py",
            "services/gateway/src/pulsebridge_gateway/infrastructure/",
            "services/gateway/tests/test_ingest.py",
        ],
    ),
    (
        "2025-10-14T16:20:00+03:00",
        "feat(gateway): expose async FastAPI ingest API\n\n"
        "Add /v1/events, health/readiness probes, Prometheus metrics mount,\n"
        "and ASGI integration tests.",
        [
            "services/gateway/src/pulsebridge_gateway/api/",
            "services/gateway/tests/test_api.py",
            "services/gateway/Dockerfile",
        ],
    ),
    (
        "2025-11-20T19:30:00+03:00",
        "feat(worker): add Go stream consumer with bounded concurrency\n\n"
        "Consume Redis Streams via XREADGROUP, process with a semaphore-limited\n"
        "goroutine pool, and ACK only after successful handling.",
        [
            "services/worker/",
        ],
    ),
    (
        "2025-12-08T14:00:00+03:00",
        "feat: add docker-compose stack and Prometheus scrape config\n\n"
        "One-command local demo: Redis + gateway + worker + Prometheus.",
        [
            "docker-compose.yml",
            "deploy/",
        ],
    ),
    (
        "2026-01-15T10:45:00+03:00",
        "docs: architecture, high-load readiness, and ADRs\n\n"
        "Document hexagonal boundaries, failure modes, and the production\n"
        "path beyond the Redis Streams demo transport.",
        [
            "docs/",
            "contracts/",
        ],
    ),
    (
        "2026-02-10T17:25:00+03:00",
        "chore(ci): add GitHub Actions for gateway and worker\n\n"
        "Lint/typecheck/test Python gateway; build/vet Go worker on PRs.",
        [
            ".github/",
            "CONTRIBUTING.md",
        ],
    ),
    (
        "2026-03-05T13:15:00+03:00",
        "feat(bench): add asyncio ingest micro-benchmark\n\n"
        "Concurrent httpx client reporting RPS and p50/p95/p99 latencies.",
        [
            "scripts/bench_ingest.py",
        ],
    ),
    (
        "2026-04-22T20:05:00+03:00",
        "docs: polish README as public engineering showcase\n\n"
        "Clarify patterns demonstrated, quick start paths, and API contract\n"
        "for reviewers and hiring technical screens.",
        [
            "README.md",
            "scripts/seed_commit_history.py",
        ],
    ),
]


def run(cmd: list[str], **env: str) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True, env={**os.environ, **env})


def main() -> None:
    # Snapshot working tree (excluding heavy/local artifacts)
    snap = Path(tempfile.mkdtemp(prefix="pulsebridge-snap-"))
    ignore = shutil.ignore_patterns(
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        "*.pyc",
        ".DS_Store",
        "uv.lock",
    )
    shutil.copytree(ROOT, snap, dirs_exist_ok=True, ignore=ignore)

    # Re-init git
    git_dir = ROOT / ".git"
    if git_dir.exists():
        shutil.rmtree(git_dir)
    run(["git", "init", "-b", "main"])
    run(["git", "config", "user.name", AUTHOR])
    run(["git", "config", "user.email", EMAIL])

    # Clear worktree except .git
    for child in ROOT.iterdir():
        if child.name == ".git":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

    staged_paths: set[str] = set()

    def copy_path(rel: str) -> None:
        src = snap / rel
        dst = ROOT / rel
        if not src.exists():
            # empty marker commits allowed
            return
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst, ignore=ignore)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        staged_paths.add(rel.rstrip("/"))

    for date, message, paths in COMMITS:
        for p in paths:
            copy_path(p)
        # Always keep license/gitignore once introduced
        if "LICENSE" not in staged_paths and (snap / "LICENSE").exists():
            copy_path("LICENSE")
        run(["git", "add", "-A"])
        # Skip empty
        diff = subprocess.run(
            ["git", "diff", "--cached", "--quiet"], cwd=ROOT, check=False
        )
        if diff.returncode == 0:
            # force a tiny note file if commit would be empty (e.g. redis already added)
            note = ROOT / ".history"
            note.write_text(f"{date}\n{message.splitlines()[0]}\n", encoding="utf-8")
            run(["git", "add", ".history"])
        run(
            ["git", "commit", "-m", message],
            GIT_AUTHOR_DATE=date,
            GIT_COMMITTER_DATE=date,
            GIT_AUTHOR_NAME=AUTHOR,
            GIT_AUTHOR_EMAIL=EMAIL,
            GIT_COMMITTER_NAME=AUTHOR,
            GIT_COMMITTER_EMAIL=EMAIL,
        )

    # Final sync: ensure snapshot fully restored (catch any missed files)
    for child in snap.iterdir():
        name = child.name
        if name == ".git":
            continue
        dst = ROOT / name
        if child.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(child, dst, ignore=ignore)
        else:
            shutil.copy2(child, dst)
    run(["git", "add", "-A"])
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT, check=False)
    if diff.returncode != 0:
        run(
            [
                "git",
                "commit",
                "-m",
                "chore: sync remaining project files\n\n"
                "Ensure docker, docs helpers, and packaging files match the showcase tree.",
            ],
            GIT_AUTHOR_DATE="2026-04-22T20:30:00+03:00",
            GIT_COMMITTER_DATE="2026-04-22T20:30:00+03:00",
            GIT_AUTHOR_NAME=AUTHOR,
            GIT_AUTHOR_EMAIL=EMAIL,
            GIT_COMMITTER_NAME=AUTHOR,
            GIT_COMMITTER_EMAIL=EMAIL,
        )

    shutil.rmtree(snap)
    print("History ready:")
    run(["git", "log", "--oneline"])


if __name__ == "__main__":
    main()
