import asyncio
import os
import pathlib
import secrets
import stat

from hyperscale.core.runtime.real_filesystem import RealFilesystem

from .cluster_cookie_unavailable_error import ClusterCookieUnavailableError


COOKIE_DIRECTORY_NAME = "hyperscale"
COOKIE_FILE_NAME = "cluster_cookie"
# 32 random bytes, url-safe base64 encoded (secrets.token_urlsafe).
COOKIE_SECRET_BYTES = 32
# Random bytes naming a creator's private staging file.
STAGING_NAME_BYTES = 16
COOKIE_FILE_MODE = 0o600
COOKIE_DIRECTORY_MODE = 0o700
# Any permission held by the file's group or by other users (POSIX).
SHARED_PERMISSION_BITS = stat.S_IRWXG | stat.S_IRWXO


class ClusterCookie:
    """The per-user cluster cookie: a secret every hyperscale command run by
    one user shares when neither ``--acm-secret`` nor
    ``MERCURY_SYNC_AUTH_SECRET`` names one (the ``~/.erlang.cookie`` model).

    It lives in the user's configuration directory --
    ``$XDG_CONFIG_HOME/hyperscale/cluster_cookie``, else
    ``~/.config/hyperscale/cluster_cookie``; on Windows
    ``%APPDATA%\\hyperscale\\cluster_cookie`` -- and is created once with
    ``secrets.token_urlsafe(32)``, readable by its owner only (mode 0600).

    Creation is atomic: the secret is written to a private staging file
    created with ``O_CREAT | O_EXCL`` and mode 0600 (never readable by
    anyone else, not even briefly), then published with ``os.link``, which
    refuses to replace an existing cookie. A creator that loses a race
    reads the winner's cookie, which is always complete. A cookie that
    cannot be created, read or trusted raises
    ``ClusterCookieUnavailableError``: a command never runs without a
    secret.
    """

    def __init__(self, cookie_path: pathlib.Path) -> None:
        self._cookie_path = cookie_path

    @property
    def cookie_path(self) -> pathlib.Path:
        """Where this cookie lives."""
        return self._cookie_path

    @classmethod
    async def secret_for_current_user(cls) -> str:
        """The current user's cookie secret, created on first use. The file
        work runs off the event loop.

        Raises:
            ClusterCookieUnavailableError: the cookie cannot be created,
                read or trusted.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, cls._read_or_create_for_current_user)

    @classmethod
    def _read_or_create_for_current_user(cls) -> str:
        return cls(cls.default_path()).read_or_create()

    async def secret(self) -> str:
        """This cookie's secret, created on first use. The file work runs
        off the event loop.

        Raises:
            ClusterCookieUnavailableError: the cookie cannot be created,
                read or trusted.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.read_or_create)

    @staticmethod
    def default_path() -> pathlib.Path:
        """The current user's cookie path (see the class docstring).

        Raises:
            ClusterCookieUnavailableError: the user has no configuration
                directory (no home directory, or no ``APPDATA`` on Windows).
        """
        configuration_directory = (
            ClusterCookie._windows_configuration_directory()
            if os.name == "nt"
            else ClusterCookie._posix_configuration_directory()
        )
        return configuration_directory / COOKIE_DIRECTORY_NAME / COOKIE_FILE_NAME

    @staticmethod
    def _windows_configuration_directory() -> pathlib.Path:
        if application_data := os.environ.get("APPDATA"):
            return pathlib.Path(application_data)

        raise ClusterCookieUnavailableError(
            "APPDATA is not set, so there is no per-user configuration directory"
        )

    @staticmethod
    def _posix_configuration_directory() -> pathlib.Path:
        # The XDG Base Directory specification ignores a relative value.
        configured_directory = os.environ.get("XDG_CONFIG_HOME", "")
        if os.path.isabs(configured_directory):
            return pathlib.Path(configured_directory)

        return ClusterCookie._home_configuration_directory()

    @staticmethod
    def _home_configuration_directory() -> pathlib.Path:
        try:
            home_directory = pathlib.Path.home()
        except RuntimeError as home_error:
            raise ClusterCookieUnavailableError(
                f"the home directory could not be determined ({home_error})"
            ) from home_error

        if not home_directory.is_absolute():
            raise ClusterCookieUnavailableError(
                f"the home directory {str(home_directory)!r} is not an absolute path"
            )

        return home_directory / ".config"

    def read_or_create(self) -> str:
        """This cookie's secret, created on first use. Blocking: call it
        off the event loop (``secret``/``secret_for_current_user`` do).

        Raises:
            ClusterCookieUnavailableError: the cookie cannot be created,
                read or trusted.
        """
        try:
            return self._read_or_create_cookie()
        except (OSError, UnicodeDecodeError) as cookie_error:
            raise ClusterCookieUnavailableError(
                f"{self._cookie_path}: {cookie_error}"
            ) from cookie_error

    def _read_or_create_cookie(self) -> str:
        try:
            return self._read_existing_cookie()
        except FileNotFoundError:
            return self._create_or_read_winning_cookie()

    def _create_or_read_winning_cookie(self) -> str:
        self._cookie_path.parent.mkdir(
            mode=COOKIE_DIRECTORY_MODE,
            parents=True,
            exist_ok=True,
        )
        try:
            return self._publish_new_cookie()
        except FileExistsError:
            # A concurrent creator published first: use its cookie.
            return self._read_existing_cookie()

    def _publish_new_cookie(self) -> str:
        secret = secrets.token_urlsafe(COOKIE_SECRET_BYTES)
        staging_path = self._cookie_path.with_name(
            f".{COOKIE_FILE_NAME}.{secrets.token_hex(STAGING_NAME_BYTES)}"
        )
        staging_descriptor = os.open(
            staging_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            COOKIE_FILE_MODE,
        )
        try:
            with os.fdopen(staging_descriptor, "wb") as staging_file:
                staging_file.write(secret.encode("ascii"))
                staging_file.flush()
                RealFilesystem.sync_durably(staging_file.fileno())

            os.link(staging_path, self._cookie_path)
        finally:
            staging_path.unlink()

        # The published link (and the staging file's removal) are directory
        # entries: sync the directory so a crash cannot lose the cookie the
        # commands of this boot already share. Windows has no directory
        # descriptor to sync (NTFS journals its metadata itself).
        if os.name != "nt":
            RealFilesystem.fsync_directory_sync(self._cookie_path.parent)

        return secret

    def _read_existing_cookie(self) -> str:
        cookie_descriptor = os.open(self._cookie_path, os.O_RDONLY)
        with os.fdopen(cookie_descriptor, "rb") as cookie_file:
            self._refuse_shared_cookie(os.fstat(cookie_file.fileno()))
            secret = cookie_file.read().decode("utf-8").strip()

        if not secret:
            raise ClusterCookieUnavailableError(f"{self._cookie_path} is empty")

        return secret

    def _refuse_shared_cookie(self, cookie_status: os.stat_result) -> None:
        # Windows guards the file with the ACL %APPDATA% inherits, not with
        # POSIX permission bits.
        if os.name != "nt" and cookie_status.st_mode & SHARED_PERMISSION_BITS:
            raise ClusterCookieUnavailableError(
                f"{self._cookie_path} is accessible to its group or other users "
                f"(mode {stat.S_IMODE(cookie_status.st_mode):o}); "
                f"restrict it to its owner with: chmod 600 {self._cookie_path}"
            )
