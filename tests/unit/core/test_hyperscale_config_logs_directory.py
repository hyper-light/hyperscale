"""The config creates a missing logs directory, parents included.

``logs_directory`` was a pydantic ``DirectoryPath``, which rejects a path
that does not exist before the model validator meant to create it runs: a
config naming a fresh directory (a new container volume) failed to parse.
"""

import pathlib

import pytest
from pydantic import ValidationError

from hyperscale.core.jobs.models.hyperscale_config import HyperscaleConfig


def test_a_missing_logs_directory_is_created_with_its_parents(tmp_path: pathlib.Path) -> None:
    logs_directory = tmp_path / "volume" / "state" / "logs"

    config = HyperscaleConfig(logs_directory=str(logs_directory))

    assert logs_directory.is_dir()
    assert config.logs_directory == str(logs_directory.resolve())


def test_an_existing_logs_directory_is_kept(tmp_path: pathlib.Path) -> None:
    config = HyperscaleConfig(logs_directory=tmp_path)

    assert config.logs_directory == str(tmp_path.resolve())


def test_a_logs_directory_that_is_a_file_is_refused(tmp_path: pathlib.Path) -> None:
    occupied = tmp_path / "logs"
    occupied.write_text("not a directory")

    with pytest.raises(ValidationError, match="not a directory"):
        HyperscaleConfig(logs_directory=str(occupied))
