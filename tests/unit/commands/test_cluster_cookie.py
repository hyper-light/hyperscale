"""
The per-user cluster cookie and the cluster secret precedence of
``hyperscale run worker|manager|gate`` and the cluster client commands:
``--acm-secret``, then ``MERCURY_SYNC_AUTH_SECRET``, then the cookie
(created once, atomically, owner-only). A cookie that cannot be created,
read or trusted fails the command with instructions -- never a run
without a secret.

Every test points the cookie at a fresh temporary configuration
directory; none touches the operator's own.
"""

import asyncio
import os
import pathlib
import stat

import pytest

from hyperscale.commands.run.cluster_cookie import ClusterCookie
from hyperscale.commands.run.cluster_cookie_unavailable_error import ClusterCookieUnavailableError
from hyperscale.commands.run.shared import resolve_auth_secret

AUTH_SECRET_ENVAR = "MERCURY_SYNC_AUTH_SECRET"
FLAG_SECRET = "cluster-cookie-flag-secret-0123456789"
ENVIRONMENT_SECRET = "cluster-cookie-environment-secret-0123456789"
# secrets.token_urlsafe(32): 32 bytes, base64url without padding.
COOKIE_SECRET_LENGTH = 43
CONCURRENT_RESOLUTIONS = 32
OWNER_ONLY_MODE = 0o600
GROUP_READABLE_MODE = 0o640
READ_ONLY_DIRECTORY_MODE = 0o500

posix_permissions_only = pytest.mark.skipif(
    os.name == "nt",
    reason="POSIX permission bits; Windows guards the cookie with %APPDATA%'s ACL",
)
unprivileged_only = pytest.mark.skipif(
    os.name == "nt" or os.geteuid() == 0,
    reason="needs POSIX permissions an unprivileged user cannot bypass",
)


@pytest.fixture
def configuration_directory(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """A fresh XDG configuration directory, with no secret in the
    environment."""
    directory = tmp_path / "config"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(directory))
    monkeypatch.delenv(AUTH_SECRET_ENVAR, raising=False)
    return directory


def cookie_path_in(configuration_directory: pathlib.Path) -> pathlib.Path:
    return configuration_directory / "hyperscale" / "cluster_cookie"


async def test_cookie_is_created_owner_only_under_the_configuration_directory(
    configuration_directory: pathlib.Path,
) -> None:
    secret = await ClusterCookie.secret_for_current_user()

    cookie_path = cookie_path_in(configuration_directory)
    assert ClusterCookie.default_path() == cookie_path
    assert cookie_path.read_text() == secret
    assert len(secret) == COOKIE_SECRET_LENGTH
    if os.name != "nt":
        assert stat.S_IMODE(cookie_path.stat().st_mode) == OWNER_ONLY_MODE


async def test_cookie_is_reused_on_the_second_resolution(configuration_directory: pathlib.Path) -> None:
    first_secret = await ClusterCookie.secret_for_current_user()
    second_secret = await ClusterCookie.secret_for_current_user()

    assert first_secret == second_secret


async def test_concurrent_creators_all_end_with_the_same_secret(configuration_directory: pathlib.Path) -> None:
    secrets_resolved = await asyncio.gather(
        *[ClusterCookie.secret_for_current_user() for _ in range(CONCURRENT_RESOLUTIONS)]
    )

    cookie_path = cookie_path_in(configuration_directory)
    assert set(secrets_resolved) == {cookie_path.read_text()}
    # Every losing creator removed its private staging file.
    assert [entry.name for entry in cookie_path.parent.iterdir()] == ["cluster_cookie"]


async def test_concurrent_creators_through_resolve_auth_secret_agree(
    configuration_directory: pathlib.Path,
) -> None:
    secrets_resolved = await asyncio.gather(
        *[resolve_auth_secret(None) for _ in range(CONCURRENT_RESOLUTIONS)]
    )

    assert len(set(secrets_resolved)) == 1


@posix_permissions_only
async def test_a_group_readable_cookie_is_refused(configuration_directory: pathlib.Path) -> None:
    await ClusterCookie.secret_for_current_user()
    cookie_path = cookie_path_in(configuration_directory)
    cookie_path.chmod(GROUP_READABLE_MODE)

    with pytest.raises(ClusterCookieUnavailableError) as refusal:
        await ClusterCookie.secret_for_current_user()

    assert f"chmod 600 {cookie_path}" in str(refusal.value)


@unprivileged_only
async def test_an_unwritable_configuration_directory_fails_with_instructions(
    configuration_directory: pathlib.Path,
) -> None:
    configuration_directory.mkdir()
    configuration_directory.chmod(READ_ONLY_DIRECTORY_MODE)
    try:
        with pytest.raises(ClusterCookieUnavailableError) as refusal:
            await ClusterCookie.secret_for_current_user()

    finally:
        configuration_directory.chmod(0o700)

    assert "MERCURY_SYNC_AUTH_SECRET" in str(refusal.value)
    assert "--acm-secret" in str(refusal.value)


@unprivileged_only
async def test_an_unusable_cookie_fails_the_command_with_instructions(
    configuration_directory: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configuration_directory.mkdir()
    configuration_directory.chmod(READ_ONLY_DIRECTORY_MODE)
    try:
        with pytest.raises(SystemExit) as exit_info:
            await resolve_auth_secret(None)

    finally:
        configuration_directory.chmod(0o700)

    assert exit_info.value.code == 1
    error_output = capsys.readouterr().err
    assert "MERCURY_SYNC_AUTH_SECRET" in error_output
    assert "--acm-secret" in error_output


async def test_an_empty_cookie_is_refused(configuration_directory: pathlib.Path) -> None:
    cookie_path = cookie_path_in(configuration_directory)
    cookie_path.parent.mkdir(parents=True)
    cookie_path.touch(mode=OWNER_ONLY_MODE)

    with pytest.raises(ClusterCookieUnavailableError, match="is empty"):
        await ClusterCookie.secret_for_current_user()


async def test_a_relative_xdg_configuration_home_is_ignored(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home_directory = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home_directory))
    monkeypatch.setenv("XDG_CONFIG_HOME", "relative/config")

    assert ClusterCookie.default_path() == home_directory / ".config" / "hyperscale" / "cluster_cookie"


async def test_without_an_absolute_home_directory_the_cookie_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setenv("HOME", "relative-home")

    with pytest.raises(ClusterCookieUnavailableError, match="MERCURY_SYNC_AUTH_SECRET"):
        await ClusterCookie.secret_for_current_user()


def test_windows_cookie_lives_under_appdata(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APPDATA", str(tmp_path))

    assert ClusterCookie._windows_configuration_directory() == tmp_path


def test_windows_without_appdata_the_cookie_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("APPDATA", raising=False)

    with pytest.raises(ClusterCookieUnavailableError, match="APPDATA"):
        ClusterCookie._windows_configuration_directory()


async def test_the_flag_wins_over_the_environment_and_the_cookie(
    configuration_directory: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(AUTH_SECRET_ENVAR, ENVIRONMENT_SECRET)

    assert await resolve_auth_secret(FLAG_SECRET) == FLAG_SECRET
    assert not cookie_path_in(configuration_directory).exists(), "the cookie is never created when not needed"


async def test_the_environment_wins_over_the_cookie(
    configuration_directory: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cookie_secret = await ClusterCookie.secret_for_current_user()
    monkeypatch.setenv(AUTH_SECRET_ENVAR, ENVIRONMENT_SECRET)

    resolved_secret = await resolve_auth_secret(None)

    assert resolved_secret == ENVIRONMENT_SECRET
    assert resolved_secret != cookie_secret


async def test_the_cookie_is_used_when_neither_flag_nor_environment_names_a_secret(
    configuration_directory: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cookie_secret = await ClusterCookie.secret_for_current_user()
    monkeypatch.setenv(AUTH_SECRET_ENVAR, "")

    assert await resolve_auth_secret(None) == cookie_secret
    assert await resolve_auth_secret("") == cookie_secret
