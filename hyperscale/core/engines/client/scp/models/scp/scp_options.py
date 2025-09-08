import pathlib
from typing import Literal
from pydantic import (
    BaseModel,
    StrictStr,
    StrictBool,
    StrictInt,
    StrictFloat,
    ConfigDict,
)
from hyperscale.core.engines.client.ssh.protocol.connection import ServerHostKeysHandler


class SCPOptions(BaseModel):
    known_hosts: tuple[StrictStr] | list[StrictStr] | None = None
    kex_algs: tuple[StrictStr] | list[StrictStr] | None = None
    proxy_command: StrictStr | list[StrictStr] | None = None
    host_key_alias: StrictStr | None = None
    x509_trusted_certs: list[StrictStr] | None = None
    x509_trusted_cert_paths: list[StrictStr] | None = None
    x509_purposes: list[StrictStr] | None = None
    server_host_key_algs: list[StrictStr] | None = None
    server_host_keys_handler: ServerHostKeysHandler | None = None
    client_host_keysign: StrictBool | None = None
    client_host_keys: list[StrictStr] | None = None
    client_host_certs: list[StrictStr] | None = None
    client_host: StrictStr | None = None
    client_username: StrictStr | None = None
    client_keys: list[StrictStr] | None = None
    client_certs: list[StrictStr] | None = None
    ignore_encrypted: StrictBool | None = None
    host_based_auth: StrictBool | None = None
    public_key_auth: StrictBool | None = None
    password_auth: StrictBool | None = None
    gss_host: StrictStr | None = None
    gss_store: StrictStr | None = None
    gss_kex: StrictBool | None = None
    gss_auth: StrictBool | None = None
    preferred_auth: list[StrictStr] | None = None
    disable_trivial_auth: StrictBool | None = None
    agent_path: StrictStr | pathlib.Path | None = None
    agent_identities: list[StrictStr] | None = None
    agent_forwarding: StrictBool | None = None
    pkcs11_provider: StrictStr | pathlib.Path | None = None
    pkcs11_pin: StrictStr | None = None
    client_version: StrictStr | None = None
    encryption_algs: list[StrictStr] | None = None
    mac_algs: list[StrictStr] | None = None
    compression_algs: list[StrictStr] | None = None
    signature_algs: list[StrictStr] | None = None
    rekey_bytes: StrictInt | StrictStr | None = None
    rekey_seconds: StrictInt | StrictFloat | StrictStr | None = None
    connect_timeout: StrictInt | StrictFloat | StrictStr | None = None
    login_timeout: StrictInt | StrictFloat | StrictStr | None = None
    keepalive_interval: tuple[StrictStr | StrictInt | StrictFloat, ...] | None = None
    keepalive_count_max: StrictInt | None = None
    tcp_keepalive: StrictBool | None = None
    canonicalize_hostname: StrictBool | Literal["always"] | None = None
    canonical_domains: list[StrictStr] | None = None
    canonicalize_fallback_local: StrictBool | None = None
    canonicalize_max_dots: StrictInt | None = None
    canonicalize_permitted_cnames: list[tuple[StrictStr, StrictStr]] | None = None
    subsystem: StrictStr | None = None
    env: dict[StrictStr, StrictStr] | None = None
    send_env: list[StrictStr] | None = None
    x11_forwarding: StrictBool | Literal["ignore_failure"] | None = None
    x11_display: StrictStr | None = None
    x11_auth_path: StrictStr | pathlib.Path | None = None
    x11_single_connection: StrictBool | None = None
    encoding: StrictStr | None = None
    errors: StrictStr | None = None
    max_pktsize: StrictInt | None = None
    config: list[StrictStr] | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)
