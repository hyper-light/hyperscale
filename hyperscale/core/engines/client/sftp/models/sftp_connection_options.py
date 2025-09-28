from typing import Literal, Any

from hyperscale.core.engines.client.sftp.protocols.sftp import MIN_SFTP_VERSION


SFTPVersion = Literal["v3", "v4", "v5", "v6"]


class SFTPConnectionOptions:

    __slots__ = (
        "minimum_sftp_version",
        "path_encoding",
        "env",
        "remote_env",
        "connection_options",
    )

    def __init__(
        self,
        sftp_version: SFTPVersion = "v3",
        path_encoding: str = 'utf-8',   
        env: dict[str, str] | None = None,
        remote_env: dict[str, str] | None = None,
        options: dict[str, Any] | None = None,
    ):
        
        if options is None:
            options = {}
        
        self._sftp_version: dict[SFTPVersion, int] = {
            "v3": MIN_SFTP_VERSION,
            "v4": 4,
            "v5": 5,
            "v6": 6,
        }

        self.sftp_version = self._sftp_version[sftp_version]
        self.path_encoding = path_encoding
        self.env = env
        self.remote_env = remote_env
        self.options = options