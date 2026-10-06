#!/usr/bin/env bash
set -euo pipefail

git config --global --add safe.directory /workspace

# The project environment exactly as uv.lock pins it: every optional
# client and reporter, plus the dev group (pytest and the lint tools).
uv sync --all-extras --dev

uv run playwright install --with-deps
