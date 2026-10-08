"""
Fixtures shared by the in-process integration tests.
"""

import pathlib
from collections.abc import Iterator

import pytest

from hyperscale.logging.config.logging_config import _global_logging_directory


@pytest.fixture
def node_directory(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[pathlib.Path]:
    """The test's own directory for everything its nodes write: the
    working directory (log streams with no configured directory fall back
    to it) and the global logging directory both point at ``tmp_path``
    for the test, and are restored after it."""
    monkeypatch.chdir(tmp_path)
    logging_directory_token = _global_logging_directory.set(str(tmp_path))
    yield tmp_path
    _global_logging_directory.reset(logging_directory_token)
