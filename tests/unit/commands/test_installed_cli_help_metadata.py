"""
The CLI's help metadata (the project name and version every command's help
shows) for an installed Hyperscale.

Every command builds its help text when it is defined, at import, and the
metadata came only from the pyproject.toml found by walking up from the
calling module. An installed package has none above it, so an installed
``hyperscale`` -- from PyPI, a wheel or ``uv tool install`` -- failed at
import, before any command could run, ``--help`` included.

* an installed distribution answers from its metadata;
* an editable install answers from its source's pyproject.toml, which its
  install-time metadata may predate;
* the help text a command builds names the installed version.
"""

import importlib.metadata
import tomllib
from pathlib import Path

from hyperscale.commands.cli.help_message.project import find_pyproject_toml

REPOSITORY_PYPROJECT = Path(__file__).resolve().parents[3] / "pyproject.toml"


def metadata_as_a_hyperscale_command_asks() -> dict:
    """The lookup as a command module of the hyperscale package calls it --
    the frame it inspects is that module's."""
    return eval(
        "find_caller_relative_path_to_pyproject()",
        {
            "__name__": "hyperscale.commands.run.workflow",
            "__file__": find_pyproject_toml.__file__,
            "find_caller_relative_path_to_pyproject": find_pyproject_toml.find_caller_relative_path_to_pyproject,
        },
    )


def test_an_installed_distribution_answers_from_its_metadata(monkeypatch) -> None:
    monkeypatch.setattr(find_pyproject_toml, "_answers_from_metadata", lambda distribution: distribution is not None)

    metadata = metadata_as_a_hyperscale_command_asks()

    assert metadata == {
        "project": {
            "name": importlib.metadata.metadata("hyperscale")["Name"],
            "version": importlib.metadata.version("hyperscale"),
        }
    }


def test_an_editable_install_answers_from_its_source_pyproject(monkeypatch) -> None:
    monkeypatch.setattr(find_pyproject_toml, "_answers_from_metadata", lambda distribution: False)

    metadata = metadata_as_a_hyperscale_command_asks()

    with REPOSITORY_PYPROJECT.open("rb") as pyproject_file:
        assert metadata["project"]["version"] == tomllib.load(pyproject_file)["project"]["version"]


def test_only_an_installed_non_editable_distribution_answers_from_metadata() -> None:
    class Distribution:
        def __init__(self, direct_url: str | None) -> None:
            self._direct_url = direct_url

        def read_text(self, filename: str) -> str | None:
            return self._direct_url if filename == "direct_url.json" else None

    assert not find_pyproject_toml._answers_from_metadata(
        Distribution('{"url": "file:///src", "dir_info": {"editable": true}}')
    )
    assert find_pyproject_toml._answers_from_metadata(Distribution('{"url": "file:///src", "dir_info": {}}'))
    assert find_pyproject_toml._answers_from_metadata(Distribution(None))
    assert not find_pyproject_toml._answers_from_metadata(None)
