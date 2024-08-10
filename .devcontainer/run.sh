#!/usr/bin/env bash
git config --global --add safe.directory /workspace
if [[ ! -d /workspace/.venv ]]; then
    uv venv
fi

pip uninstall --root-user-action=ignore -y playwright

. /workspace/.venv/bin/activate && \
uv pip install -r requirements.txt && \
playwright install --with-deps && \
uv pip install -e .
