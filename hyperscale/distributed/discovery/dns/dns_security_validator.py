"""``DNSSecurityValidator`` -- pickled under the namespace
``hyperscale.distributed.discovery.dns.security`` (see that module)."""

import ipaddress
from dataclasses import dataclass, field

from .security_shared import _DEFAULT_CLOCK
from .dns_security_event import DNSSecurityEvent
from .dns_security_violation import DNSSecurityViolation
from .host_history import HostHistory


@dataclass
class DNSSecurityValidator:
    """
    Validates DNS resolution results for security.

    Features:
    - IP range validation against allowed CIDRs
    - Anomaly detection for IP changes
    - Fast-flux detection (rapid IP rotation)
    - DNS rebinding protection

    Usage:
        validator = DNSSecurityValidator(
            allowed_cidrs=["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
        )

        # Validate a resolution result
        violation = validator.validate("manager.local", "10.0.1.5")
        if violation:
            logger.warning(f"DNS security: {violation.details}")
    """

    allowed_cidrs: list[str] = field(default_factory=list)
    """List of allowed CIDR ranges for resolved IPs.

    Empty list means all IPs are allowed (validation disabled).
    Example: ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
    """

    block_private_for_public: bool = False
    """Block private IPs (RFC1918) for public hostnames.

    When True, if a hostname doesn't end with .local, .internal, .svc,
    or similar internal TLDs, private IPs will be rejected.
    This helps prevent DNS rebinding attacks.
    """

    detect_ip_changes: bool = True
    """Enable detection of unexpected IP changes."""

    max_ip_changes_per_window: int = 5
    """Maximum IP changes allowed in the tracking window.

    More changes than this triggers a rapid rotation alert.
    """

    ip_change_window_seconds: float = 300.0
    """Time window for tracking IP changes (5 minutes default)."""

    _parsed_networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = field(
        default_factory=list, repr=False
    )
    """Parsed network objects for CIDR validation."""

    _private_networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = field(
        default_factory=list, repr=False, init=False
    )
    """RFC1918 private networks for rebinding detection."""

    _host_history: dict[str, HostHistory] = field(default_factory=dict, repr=False)
    """Historical IP data per hostname."""

    _security_events: list[DNSSecurityEvent] = field(default_factory=list, repr=False)
    """Recent security events for monitoring."""

    max_events: int = 1000
    """Maximum security events to retain."""

    _internal_tlds: frozenset[str] = field(
        default_factory=lambda: frozenset([
            ".local", ".internal", ".svc", ".cluster.local",
            ".corp", ".home", ".lan", ".private", ".test",
        ]),
        repr=False,
        init=False,
    )
    """TLDs considered internal (won't trigger rebinding alerts)."""

    def __post_init__(self) -> None:
        """Parse CIDR strings into network objects."""
        self._parsed_networks = []
        for cidr in self.allowed_cidrs:
            try:
                network = ipaddress.ip_network(cidr, strict=False)
                self._parsed_networks.append(network)
            except ValueError as exc:
                raise ValueError(f"Invalid CIDR '{cidr}': {exc}") from exc

        # Pre-parse private networks for rebinding check
        self._private_networks = [
            ipaddress.ip_network("10.0.0.0/8"),
            ipaddress.ip_network("172.16.0.0/12"),
            ipaddress.ip_network("192.168.0.0/16"),
            ipaddress.ip_network("127.0.0.0/8"),
            ipaddress.ip_network("169.254.0.0/16"),  # Link-local
            ipaddress.ip_network("fc00::/7"),  # IPv6 unique local
            ipaddress.ip_network("fe80::/10"),  # IPv6 link-local
            ipaddress.ip_network("::1/128"),  # IPv6 loopback
        ]

    def validate(
        self,
        hostname: str,
        resolved_ip: str,
    ) -> DNSSecurityEvent | None:
        """
        Validate a DNS resolution that answered one address.

        Args:
            hostname: The hostname that was resolved
            resolved_ip: The IP address returned by DNS

        Returns:
            DNSSecurityEvent if a violation is detected, None otherwise
        """
        _, events = self.validate_answer(hostname, [resolved_ip])
        return events[0] if events else None

    def _validate_address(
        self,
        hostname: str,
        resolved_ip: str,
    ) -> DNSSecurityEvent | None:
        """Check one resolved address against the range and rebinding
        policies."""
        # Parse the IP address
        try:
            ip_addr = ipaddress.ip_address(resolved_ip)
        except ValueError:
            # Invalid IP format - this is a serious error
            event = DNSSecurityEvent(
                hostname=hostname,
                violation_type=DNSSecurityViolation.IP_OUT_OF_RANGE,
                resolved_ip=resolved_ip,
                details=f"Invalid IP format: {resolved_ip}",
            )
            self._record_event(event)
            return event

        # Check CIDR ranges if configured
        if self._parsed_networks:
            in_allowed_range = any(
                ip_addr in network for network in self._parsed_networks
            )
            if not in_allowed_range:
                event = DNSSecurityEvent(
                    hostname=hostname,
                    violation_type=DNSSecurityViolation.IP_OUT_OF_RANGE,
                    resolved_ip=resolved_ip,
                    details=f"IP {resolved_ip} not in allowed ranges: {self.allowed_cidrs}",
                )
                self._record_event(event)
                return event

        # Check for DNS rebinding (private IP for public hostname)
        if self.block_private_for_public:
            if not self._is_internal_hostname(hostname):
                is_private = any(
                    ip_addr in network for network in self._private_networks
                )
                if is_private:
                    event = DNSSecurityEvent(
                        hostname=hostname,
                        violation_type=DNSSecurityViolation.PRIVATE_IP_FOR_PUBLIC_HOST,
                        resolved_ip=resolved_ip,
                        details=f"Private IP {resolved_ip} returned for public hostname '{hostname}'",
                    )
                    self._record_event(event)
                    return event

        return None

    def validate_answer(
        self,
        hostname: str,
        resolved_ips: list[str],
    ) -> tuple[list[str], list[DNSSecurityEvent]]:
        """
        Validate one resolution's answer: each address against the range
        and rebinding policies, and the answer as a whole for change
        anomalies. A multi-address answer -- a headless Service's pods --
        is one observation, not a change per address.

        Args:
            hostname: The hostname that was resolved
            resolved_ips: The answer's addresses

        Returns:
            The addresses that pass, and the violations found. An answer
            that is a rapid rotation passes no address.
        """
        events = [
            event
            for resolved_ip in resolved_ips
            if (event := self._validate_address(hostname, resolved_ip)) is not None
        ]
        rejected_ips = {event.resolved_ip for event in events}
        accepted_ips = [
            resolved_ip for resolved_ip in resolved_ips if resolved_ip not in rejected_ips
        ]

        if self.detect_ip_changes and (
            anomaly := self._check_answer_anomaly(hostname, frozenset(resolved_ips))
        ) is not None:
            self._record_event(anomaly)
            events.append(anomaly)
            accepted_ips = []

        return accepted_ips, events

    def validate_batch(
        self,
        hostname: str,
        resolved_ips: list[str],
    ) -> list[DNSSecurityEvent]:
        """
        Validate multiple IP addresses from a DNS resolution.

        Args:
            hostname: The hostname that was resolved
            resolved_ips: List of IP addresses returned

        Returns:
            List of security events (empty if all IPs are valid)
        """
        _, events = self.validate_answer(hostname, resolved_ips)
        return events

    def filter_valid_ips(
        self,
        hostname: str,
        resolved_ips: list[str],
    ) -> list[str]:
        """
        Filter a list of IPs to only those that pass validation.

        Args:
            hostname: The hostname that was resolved
            resolved_ips: List of IP addresses to filter

        Returns:
            List of valid IP addresses
        """
        accepted_ips, _ = self.validate_answer(hostname, resolved_ips)
        return accepted_ips

    def _is_internal_hostname(self, hostname: str) -> bool:
        """Check if a hostname is considered internal."""
        hostname_lower = hostname.lower()
        return any(hostname_lower.endswith(tld) for tld in self._internal_tlds)

    def _check_answer_anomaly(
        self,
        hostname: str,
        answer: frozenset[str],
    ) -> DNSSecurityEvent | None:
        """
        Detect rapid rotation (possible fast-flux) across answers.

        An answer changes only when it shares no address with the previous
        one -- the fast-flux signature. Answers that overlap, as scaling
        and rolling updates of a service produce, are not changes.
        """
        now = _DEFAULT_CLOCK.monotonic()
        history = self._host_history.get(hostname)
        if history is None:
            history = HostHistory()
            self._host_history[hostname] = history

        # Check if tracking window expired
        if now - history.window_start_time > self.ip_change_window_seconds:
            history.change_count = 0
            history.window_start_time = now

        previous_answer = history.last_answer
        history.last_answer = answer
        if not previous_answer or not previous_answer.isdisjoint(answer):
            return None

        history.change_count += 1
        history.last_change_time = now
        if history.change_count <= self.max_ip_changes_per_window:
            return None

        return DNSSecurityEvent(
            hostname=hostname,
            violation_type=DNSSecurityViolation.RAPID_IP_ROTATION,
            resolved_ip=",".join(sorted(answer)),
            previous_ip=",".join(sorted(previous_answer)),
            details=(
                f"Rapid IP rotation detected for '{hostname}': "
                f"{history.change_count} disjoint answers in {self.ip_change_window_seconds}s "
                f"(limit: {self.max_ip_changes_per_window})"
            ),
        )

    def _record_event(self, event: DNSSecurityEvent) -> None:
        """Record a security event for monitoring."""
        self._security_events.append(event)
        # Trim to max size
        if len(self._security_events) > self.max_events:
            self._security_events = self._security_events[-self.max_events:]

    def get_recent_events(
        self,
        limit: int = 100,
        violation_type: DNSSecurityViolation | None = None,
    ) -> list[DNSSecurityEvent]:
        """
        Get recent security events.

        Args:
            limit: Maximum events to return
            violation_type: Filter by violation type (None = all)

        Returns:
            List of security events, most recent first
        """
        events = self._security_events
        if violation_type:
            events = [e for e in events if e.violation_type == violation_type]
        return list(reversed(events[-limit:]))

    def get_host_history(self, hostname: str) -> HostHistory | None:
        """Get IP history for a hostname."""
        return self._host_history.get(hostname)

    def clear_history(self, hostname: str | None = None) -> int:
        """
        Clear IP history.

        Args:
            hostname: Specific hostname to clear, or None for all

        Returns:
            Number of entries cleared
        """
        if hostname:
            if hostname in self._host_history:
                del self._host_history[hostname]
                return 1
            return 0
        else:
            count = len(self._host_history)
            self._host_history.clear()
            return count

    @property
    def is_enabled(self) -> bool:
        """Check if any validation is enabled."""
        return bool(self._parsed_networks) or self.block_private_for_public or self.detect_ip_changes

    @property
    def stats(self) -> dict[str, int]:
        """Get security validator statistics."""
        by_type: dict[str, int] = {}
        for event in self._security_events:
            key = event.violation_type.value
            by_type[key] = by_type.get(key, 0) + 1

        return {
            "total_events": len(self._security_events),
            "tracked_hosts": len(self._host_history),
            "allowed_networks": len(self._parsed_networks),
            **by_type,
        }
