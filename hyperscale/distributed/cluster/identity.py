"""
ClusterIdentity — the irreducible per-node configuration (AD-52 §3).

Holds cluster_id, role, mTLS material, advertised_address, and the
process-local uuid4() runtime node_id. Loads + validates the cert chain
at construction. Hot-reloads via mtime watch so cert-manager rotation
needs no pod restart.

Hard requirements from AD-52 §3:
  - node_id is never persisted, never reused, never inferred from any
    external source. Restart is always a new join.
  - Cert subject organizationalUnit must declare the matching role per
    the AD-28 connection matrix.
  - cluster_uuid is NOT here — it is bootstrap-minted and lives in the
    Raft state machine (ClusterMetadata).
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import NameOID

if TYPE_CHECKING:
    from hyperscale.logging import Logger


_ALLOWED_ROLES: frozenset[str] = frozenset({"gate", "manager", "worker"})


class ClusterIdentity:
    """
    Process-local cluster identity. Single instance per node, constructed
    at startup from the CLI flags.

    Fields are immutable after construction except for the parsed cert
    chain, which can be reloaded by calling reload_credentials() when
    the underlying files change. The runtime node_id is generated once
    per process and can never be regenerated.
    """

    __slots__ = (
        "_cluster_id",
        "_role",
        "_mtls_cert_path",
        "_mtls_key_path",
        "_mtls_ca_path",
        "_advertised_address",
        "_node_id",
        "_cert_chain",
        "_private_key",
        "_ca_bundle",
        "_cert_mtime_ns",
        "_key_mtime_ns",
        "_ca_mtime_ns",
        "_logger",
        "_reload_lock",
    )

    def __init__(
        self,
        cluster_id: str,
        role: str,
        mtls_cert_path: str,
        mtls_key_path: str,
        mtls_ca_path: str,
        advertised_address: tuple[str, int] | None = None,
        logger: "Logger | None" = None,
    ) -> None:
        if not cluster_id:
            raise ValueError("cluster_id is required")
        if role not in _ALLOWED_ROLES:
            raise ValueError(
                f"role must be one of {sorted(_ALLOWED_ROLES)}, got {role!r}"
            )
        for label, path in (
            ("mtls_cert_path", mtls_cert_path),
            ("mtls_key_path", mtls_key_path),
            ("mtls_ca_path", mtls_ca_path),
        ):
            if not path:
                raise ValueError(f"{label} is required")
            if not Path(path).exists():
                raise ValueError(f"{label} {path!r} does not exist")

        self._cluster_id = cluster_id
        self._role = role
        self._mtls_cert_path = mtls_cert_path
        self._mtls_key_path = mtls_key_path
        self._mtls_ca_path = mtls_ca_path
        self._advertised_address = advertised_address
        self._node_id = uuid.uuid4().hex
        self._logger = logger

        self._cert_chain: list[x509.Certificate] = []
        self._private_key: object | None = None
        self._ca_bundle: list[x509.Certificate] = []
        self._cert_mtime_ns: int = 0
        self._key_mtime_ns: int = 0
        self._ca_mtime_ns: int = 0
        self._reload_lock = asyncio.Lock()

        # Initial load + role-matches-cert-subject validation.
        self._load_credentials_blocking()

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    @property
    def role(self) -> str:
        return self._role

    @property
    def node_id(self) -> str:
        """uuid4() runtime id (AD-52 §3). Stable for the process lifetime."""
        return self._node_id

    @property
    def advertised_address(self) -> tuple[str, int] | None:
        return self._advertised_address

    @property
    def cert_chain(self) -> list[x509.Certificate]:
        return list(self._cert_chain)

    @property
    def ca_bundle(self) -> list[x509.Certificate]:
        return list(self._ca_bundle)

    @property
    def private_key(self) -> object | None:
        return self._private_key

    @property
    def mtls_cert_path(self) -> str:
        return self._mtls_cert_path

    @property
    def mtls_key_path(self) -> str:
        return self._mtls_key_path

    @property
    def mtls_ca_path(self) -> str:
        return self._mtls_ca_path

    async def reload_credentials(self) -> bool:
        """
        Re-read cert/key/CA files if any mtime has advanced. Returns True
        if any file was reloaded. Safe to call concurrently; serialized
        via _reload_lock.
        """
        async with self._reload_lock:
            running_loop = asyncio.get_running_loop()
            try:
                file_stats = await running_loop.run_in_executor(
                    None,
                    self._stat_credential_files,
                )
            except FileNotFoundError:
                return False
            cert_mtime, key_mtime, ca_mtime = file_stats
            if (
                cert_mtime == self._cert_mtime_ns
                and key_mtime == self._key_mtime_ns
                and ca_mtime == self._ca_mtime_ns
            ):
                return False
            await running_loop.run_in_executor(None, self._load_credentials_blocking)
            return True

    async def watch_for_rotation(
        self,
        poll_interval_seconds: float = 30.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """
        Long-running watch task. Polls credential files every
        poll_interval_seconds and reloads on mtime change. Submit via
        TaskRunner; the loop respects stop_event for shutdown.

        Per CLAUDE.md the logger is async so individual reload events
        are logged via the optional self._logger when present.
        """
        if stop_event is None:
            stop_event = asyncio.Event()
        while not stop_event.is_set():
            try:
                changed = await self.reload_credentials()
                if changed and self._logger is not None:
                    await self._logger.log({
                        "event": "ClusterIdentityCredentialsReloaded",
                        "cluster_id": self._cluster_id,
                        "role": self._role,
                    })
            except Exception as reload_error:
                if self._logger is not None:
                    await self._logger.log({
                        "event": "ClusterIdentityReloadFailed",
                        "error": str(reload_error),
                    })
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)
            except asyncio.TimeoutError:
                continue

    def _stat_credential_files(self) -> tuple[int, int, int]:
        cert_mtime = os.stat(self._mtls_cert_path).st_mtime_ns
        key_mtime = os.stat(self._mtls_key_path).st_mtime_ns
        ca_mtime = os.stat(self._mtls_ca_path).st_mtime_ns
        return cert_mtime, key_mtime, ca_mtime

    def _load_credentials_blocking(self) -> None:
        cert_bytes = Path(self._mtls_cert_path).read_bytes()
        key_bytes = Path(self._mtls_key_path).read_bytes()
        ca_bytes = Path(self._mtls_ca_path).read_bytes()

        cert_chain = x509.load_pem_x509_certificates(cert_bytes)
        if not cert_chain:
            raise ValueError(
                f"mTLS cert file {self._mtls_cert_path!r} contained no certificates"
            )
        ca_bundle = x509.load_pem_x509_certificates(ca_bytes)
        if not ca_bundle:
            raise ValueError(
                f"mTLS CA file {self._mtls_ca_path!r} contained no certificates"
            )
        private_key = serialization.load_pem_private_key(key_bytes, password=None)

        # AD-28 connection matrix: cert subject organizationalUnit
        # must declare the matching role. Anything else is refused at
        # process start — we never trust a config that says "I'm a gate"
        # while the cert says "I'm a worker."
        leaf_certificate = cert_chain[0]
        organizational_units = [
            attr.value
            for attr in leaf_certificate.subject.get_attributes_for_oid(
                NameOID.ORGANIZATIONAL_UNIT_NAME
            )
        ]
        if self._role not in organizational_units:
            raise ValueError(
                f"mTLS cert subject OU {organizational_units!r} does not "
                f"include declared role {self._role!r} (AD-28). Operator "
                f"must issue a cert with OU={self._role}."
            )

        self._cert_chain = cert_chain
        self._private_key = private_key
        self._ca_bundle = ca_bundle
        cert_mtime, key_mtime, ca_mtime = self._stat_credential_files()
        self._cert_mtime_ns = cert_mtime
        self._key_mtime_ns = key_mtime
        self._ca_mtime_ns = ca_mtime
