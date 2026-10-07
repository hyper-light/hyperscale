"""`run workflow`'s path argument loads exactly the workflows it names.

It used to collect every Workflow subclass in the process, so a template
workflow the CLI had imported (1000 VUs against httpbin.org) ran beside
every test. A file now contributes the workflows it defines or imports; a
directory contributes every Python file under it.
"""

import pathlib
import textwrap

import pytest

from hyperscale.commands.cli.arg_types.data_types.import_type import ImportType
from hyperscale.graph import Workflow


class LeakedIntoTheProcess(Workflow):
    """A workflow that exists in the process but in no file passed to it."""


def write_module(path: pathlib.Path, source: str) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source))
    return path


async def load(path: pathlib.Path) -> dict[str, type[Workflow]] | Exception:
    import_type = ImportType(ImportType[Workflow])
    result = await import_type.parse(str(path))
    return result if isinstance(result, Exception) else import_type.data


@pytest.mark.asyncio
async def test_a_file_yields_only_its_own_workflows(tmp_path: pathlib.Path) -> None:
    test_file = write_module(
        tmp_path / "suite_a" / "load_test.py",
        """
        from hyperscale.graph import Workflow

        class CheckoutTest(Workflow):
            pass
        """,
    )

    loaded = await load(test_file)

    assert list(loaded) == ["CheckoutTest"]


@pytest.mark.asyncio
async def test_an_entry_file_runs_the_workflows_it_imports_but_not_their_bases(tmp_path: pathlib.Path) -> None:
    package = tmp_path / "suite_b"
    write_module(package / "__init__.py", "")
    write_module(
        package / "flows.py",
        """
        from hyperscale.graph import Workflow

        class SharedBase(Workflow):
            pass

        class SearchTest(SharedBase):
            pass
        """,
    )
    entry = write_module(
        package / "entry.py",
        """
        from suite_b.flows import SearchTest, SharedBase

        class BrowseTest(SharedBase):
            pass
        """,
    )

    loaded = await load(entry)

    assert sorted(loaded) == ["BrowseTest", "SearchTest"]


@pytest.mark.asyncio
async def test_a_directory_yields_every_file_under_it(tmp_path: pathlib.Path) -> None:
    suite = tmp_path / "suite_c"
    write_module(suite / "first.py", "from hyperscale.graph import Workflow\n\nclass FirstTest(Workflow):\n    pass\n")
    write_module(suite / "nested" / "second.py", "from hyperscale.graph import Workflow\n\nclass SecondTest(Workflow):\n    pass\n")
    write_module(suite / "__pycache__" / "stale.py", "raise RuntimeError('a cache file was imported')\n")

    loaded = await load(suite)

    assert sorted(loaded) == ["FirstTest", "SecondTest"]


@pytest.mark.asyncio
async def test_two_different_workflows_sharing_a_name_are_refused(tmp_path: pathlib.Path) -> None:
    suite = tmp_path / "suite_d"
    for name in ("one.py", "two.py"):
        write_module(suite / name, "from hyperscale.graph import Workflow\n\nclass SameName(Workflow):\n    pass\n")

    loaded = await load(suite)

    assert isinstance(loaded, Exception)
    assert "two classes named SameName" in str(loaded)


@pytest.mark.asyncio
async def test_a_file_without_workflows_is_refused(tmp_path: pathlib.Path) -> None:
    empty = write_module(tmp_path / "suite_e" / "helpers.py", "VALUE = 1\n")

    loaded = await load(empty)

    assert isinstance(loaded, Exception)
    assert "no Workflow classes" in str(loaded)


@pytest.mark.asyncio
async def test_a_missing_path_is_refused(tmp_path: pathlib.Path) -> None:
    loaded = await load(tmp_path / "suite_f" / "missing.py")

    assert isinstance(loaded, Exception)
