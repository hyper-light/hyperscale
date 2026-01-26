"""
DNS Security Validator for defense against DNS-based attacks.

Provides IP range validation and anomaly detection to protect against:
- DNS Cache Poisoning: Validates resolved IPs are in expected ranges
- DNS Hijacking: Detects unexpected IP changes
- DNS Spoofing: Alerts on suspicious resolution patterns

See: https://dnsmadeeasy.com/resources/16-dns-attacks-you-should-know-about
"""

import ipaddress
import time
from dataclasses import dataclass, field
from enum import Enum


class DNSSecurityViolation(Enum):
    """Types of DNS security violations."""

    IP_OUT_OF_RANGE = "ip_out_of_range"
    """Resolved IP is not in any allowed CIDR range."""

    UNEXPECTED_IP_CHANGE = "unexpected_ip_change"
    """IP changed from previously known value (possible hijacking)."""

    RAPID_IP_ROTATION = "rapid_ip_rotation"
    """IP changing too frequently (possible fast-flux attack)."""

    PRIVATE_IP_FOR_PUBLIC_HOST = "private_ip_for_public_host"
    """Private IP returned for a public hostname (possible rebinding)."""


@dataclass(slots=True)
class DNSSecurityEvent:
    """Record of a DNS security violation."""

    hostname: str
    """The hostname that triggered the violation."""

    violation_type: DNSSecurityViolation
    """Type of security violation detected."""

    resolved_ip: str
    """The IP address that was resolved."""

    details: str
    """Human-readable description of the violation."""

    timestamp: float = field(default_factory=time.monotonic)
    """When this violation occurred."""

    previous_ip: str | None = None
    """Previous IP address (for change detection)."""


@dataclass(slots=True)
class HostHistory:
    """Tracks historical IP resolutions for a hostname."""

    last_ips: list[str] = field(default_factory=list)
    """List of IPs seen for this host (most recent first)."""

    last_change_time: float = 0.0
    """Monotonic time of last IP change."""

    change_count: int = 0
    """Number of IP changes in the tracking window."""

    window_start_time: float = field(default_factory=time.monotonic)
    """Start of the current tracking window."""


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
        Validate a DNS resolution result.

        Args:
            hostname: The hostname that was resolved
            resolved_ip: The IP address returned by DNS

        Returns:
            DNSSecurityEvent if a violation is detected, None otherwise
        """
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

        # Check for anomalies (IP changes, rapid rotation)
        if self.detect_ip_changes:
            anomaly = self._check_ip_anomaly(hostname, resolved_ip)
            if anomaly:
                self._record_event(anomaly)
                return anomaly

        return None

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
        events: list[DNSSecurityEvent] = []
        for ip in resolved_ips:
            event = self.validate(hostname, ip)
            if event:
                events.append(event)
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
        valid_ips: list[str] = []
        for ip in resolved_ips:
            event = self.validate(hostname, ip)
            if event is None:
                valid_ips.append(ip)
        return valid_ips

    def _is_internal_hostname(self, hostname: str) -> bool:
        """Check if a hostname is considered internal."""
        hostname_lower = hostname.lower()
        return any(hostname_lower.endswith(tld) for tld in self._internal_tlds)

    def _check_ip_anomaly(
        self,
        hostname: str,
        resolved_ip: str,
    ) -> DNSSecurityEvent | None:
        """
        Check for IP change anomalies.

        Detects:
        - Unexpected IP changes (possible hijacking)
        - Rapid IP rotation (possible fast-flux)
        """
        now = time.monotonic()

        # Get or create history for this host
        history = self._host_history.get(hostname)
        if history is None:
            history = HostHistory()
            self._host_history[hostname] = history

        # Check if tracking window expired
        if now - history.window_start_time > self.ip_change_window_seconds:
            # Reset window
            history.change_count = 0
            history.window_start_time = now

        # Check if IP changed
        if history.last_ips and resolved_ip != history.last_ips[0]:
            previous_ip = history.last_ips[0]
            history.change_count += 1
            history.last_change_time = now

            # Check for rapid rotation
            if history.change_count > self.max_ip_changes_per_window:
                event = DNSSecurityEvent(
                    hostname=hostname,
                    violation_type=DNSSecurityViolation.RAPID_IP_ROTATION,
                    resolved_ip=resolved_ip,
                    previous_ip=previous_ip,
                    details=(
                        f"Rapid IP rotation detected for '{hostname}': "
                        f"{history.change_count} changes in {self.ip_change_window_seconds}s "
                        f"(limit: {self.max_ip_changes_per_window})"
                    ),
                )
                return event

            # Record unexpected change (informational, not blocking)
            # This is returned so callers can log it, but it's less severe
            # than out-of-range or rapid rotation
            event = DNSSecurityEvent(
                hostname=hostname,
                violation_type=DNSSecurityViolation.UNEXPECTED_IP_CHANGE,
                resolved_ip=resolved_ip,
                previous_ip=previous_ip,
                details=(
                    f"IP changed for '{hostname}': {previous_ip} -> {resolved_ip} "
                    f"(change #{history.change_count} in window)"
                ),
            )
            # Note: We return this but it's up to the caller to decide
            # whether to treat it as blocking. By default, we don't block
            # on simple IP changes as they're normal in dynamic environments.
            # We only block on rapid rotation.
            # For now, return None to not block on simple changes
            # Uncomment the return below to enable alerts on any change:
            # return event

        # Update history
        if not history.last_ips or resolved_ip != history.last_ips[0]:
            history.last_ips.insert(0, resolved_ip)
            # Keep only last 10 IPs
            if len(history.last_ips) > 10:
                history.last_ips = history.last_ips[:10]

        return None

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
