"""The CLI never imports the `hyperscale new` template as a Workflow.

`run workflow` collects Workflow subclasses after importing the user's
file, so any Workflow the CLI process had imported joined every run: the
template's (1000 VUs against httpbin.org for a minute) ran beside the
user's own test, local or cluster. The template is read as text instead.
"""

import asyncio
import pathlib
import subprocess
import sys
from importlib import resources

import pytest

from hyperscale.commands.new import TEMPLATE_FILE, TEMPLATE_PACKAGE, create_test

ENTRY_POINT_PROBE = """
import sys
import hyperscale.commands.root
from hyperscale.graph import Workflow
print(sorted(f"{cls.__module__}.{cls.__qualname__}" for cls in Workflow.__subclasses__()))
print("hyperscale.commands.workflow.test" in sys.modules)
"""


def test_loading_the_cli_defines_no_workflow() -> None:
    probe = subprocess.run(
        [sys.executable, "-c", ENTRY_POINT_PROBE],
        capture_output=True,
        text=True,
        check=True,
    )

    subclasses_line, template_loaded_line = probe.stdout.strip().splitlines()[-2:]
    assert subclasses_line == "[]", probe.stdout
    assert template_loaded_line == "False", probe.stdout


@pytest.mark.asyncio
async def test_new_writes_the_template_source_verbatim(tmp_path: pathlib.Path) -> None:
    target = tmp_path / "test.py"

    await create_test(asyncio.get_running_loop(), str(target))

    expected = resources.files(TEMPLATE_PACKAGE).joinpath(TEMPLATE_FILE).read_text()
    assert target.read_text() == expected
