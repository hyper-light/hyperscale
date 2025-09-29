import msgspec
import pathlib
from typing import Literal
from hyperscale.core.engines.client.ssh.protocol.ssh.connection import ServerHostKeysHandler


class ConnectionOptions(msgspec.Struct):
    known_hosts: tuple[str] | list[str] | None = None
    kex_algs: tuple[str] | list[str] | None = None
    proxy_command: str | list[str] | None = None
    host_key_alias: str | None = None
    x509_trusted_certs: list[str] | None = None
    x509_trusted_cert_paths: list[str] | None = None
    x509_purposes: list[str] | None = None
    server_host_key_algs: list[str] | None = None
    server_host_keys_handler: ServerHostKeysHandler | None = None
    client_host_keysign: bool | None = None
    client_host_keys: list[str] | None = None
    client_host_certs: list[str] | None = None
    client_host: str | None = None
    client_username: str | None = None
    client_keys: list[str] | None = None
    client_certs: list[str] | None = None
    ignore_encrypted: bool | None = None
    host_based_auth: bool | None = None
    public_key_auth: bool | None = None
    password_auth: bool | None = None
    gss_host: str | None = None
    gss_store: str | None = None
    gss_kex: bool | None = None
    gss_auth: bool | None = None
    preferred_auth: list[str] | None = None
    disable_trivial_auth: bool | None = None
    agent_path: str | pathlib.Path | None = None
    agent_identities: list[str] | None = None
    agent_forwarding: bool | None = None
    pkcs11_provider: str | pathlib.Path | None = None
    pkcs11_pin: str | None = None
    client_version: str | None = None
    encryption_algs: list[str] | None = None
    mac_algs: list[str] | None = None
    compression_algs: list[str] | None = None
    signature_algs: list[str] | None = None
    rekey_bytes: int | str | None = None
    rekey_seconds: int | float | str | None = None
    connect_timeout: int | float | str | None = None
    login_timeout: int | float | str | None = None
    keepalive_interval: tuple[str | int | float, ...] | None = None
    keepalive_count_max: int | None = None
    tcp_keepalive: bool | None = None
    canonicalize_hostname: bool | Literal["always"] | None = None
    canonical_domains: list[str] | None = None
    canonicalize_fallback_local: bool | None = None
    canonicalize_max_dots: int | None = None
    canonicalize_permitted_cnames: list[tuple[str, str]] | None = None
    subsystem: str | None = None
    env: dict[str, str] | None = None
    send_env: list[str] | None = None
    x11_forwarding: bool | Literal["ignore_failure"] | None = None
    x11_display: str | None = None
    x11_auth_path: str | pathlib.Path | None = None
    x11_single_connection: bool | None = None
    encoding: str | None = None
    errors: str | None = None
    max_pktsize: int | None = None
    config: list[str] | None = None

    def to_dict(self):
            return {
                f: getattr(self, f)
                for f in self.__struct_fields__
                if getattr(self, f) is not None
            }