
if [[ ! -d /workspace/.venv ]]; then
    uv venv
fi

pip uninstall -y playwright

source .venv/bin/activate


uv pip install -r requirements.in

playwright install
playwright install-deps

uv pip install -e .
