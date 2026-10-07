"""
A context opened with a bare log filename writes into the configured logs
directory.

``Logger.context(path="hyperscale.leader.log.json")`` resolved the bare
filename against the working directory itself, so the stream never saw a
missing directory and never applied the ``LoggingConfig`` logs directory:
a node configured with ``MERCURY_SYNC_LOGS_DIRECTORY`` still wrote its
executor logs (``hyperscale.leader.log.json``,
``hyperscale.worker.<n>.log.json``) into whatever directory it ran from.
A path naming a directory keeps it.
"""

import os
import pathlib

import pytest

from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger import Logger

LOG_FILENAME = "hyperscale.leader.log.json"


async def _log_once(path: str) -> None:
    logger = Logger()
    async with logger.context(name="bare-filename", path=path) as context:
        await context.log(Entry(message="entry", level=LogLevel.INFO))
    await logger.close()


@pytest.mark.asyncio
async def test_a_bare_filename_lands_in_the_configured_directory(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    working_directory = tmp_path / "working"
    configured_directory = tmp_path / "configured"
    working_directory.mkdir()
    configured_directory.mkdir()
    monkeypatch.chdir(working_directory)
    LoggingConfig().update(log_directory=str(configured_directory))

    await _log_once(LOG_FILENAME)

    assert os.listdir(working_directory) == []
    assert (configured_directory / LOG_FILENAME).exists()


@pytest.mark.asyncio
async def test_a_path_naming_a_directory_keeps_it(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    named_directory = tmp_path / "named"
    configured_directory = tmp_path / "configured"
    named_directory.mkdir()
    configured_directory.mkdir()
    monkeypatch.chdir(tmp_path)
    LoggingConfig().update(log_directory=str(configured_directory))

    await _log_once(str(named_directory / LOG_FILENAME))

    assert (named_directory / LOG_FILENAME).exists()
    assert os.listdir(configured_directory) == []
