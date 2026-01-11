"""
Integration tests for GateConfig (Section 15.3.3).

Tests the gate configuration dataclass and factory function.
"""

import pytest
from dataclasses import fields

from hyperscale.distributed_rewrite.nodes.gate.config import (
    GateConfig,
    create_gate_config,
)


class TestGateConfigHappyPath:
    """Tests for normal GateConfig operations."""

    def test_create_minimal_config(self):
        """Create config with minimal required parameters."""
        config = GateConfig(
            host="127.0.0.1",
            tcp_port=9000,
            udp_port=9001,
        )

        assert config.host == "127.0.0.1"
        assert config.tcp_port == 9000
        assert config.udp_port == 9001
        assert config.dc_id == "global"  # Default
        assert config.datacenter_managers == {}
        assert config.gate_peers == []

    def test_create_full_config(self):
        """Create config with all parameters."""
        dc_managers = {
            "dc-east": [("10.0.0.1", 8000), ("10.0.0.2", 8000)],
            "dc-west": [("10.0.1.1", 8000)],
        }
        dc_managers_udp = {
            "dc-east": [("10.0.0.1", 8001), ("10.0.0.2", 8001)],
            "dc-west": [("10.0.1.1", 8001)],
        }
        gate_peers = [("10.0.10.1", 9000), ("10.0.10.2", 9000)]
        gate_peers_udp = [("10.0.10.1", 9001), ("10.0.10.2", 9001)]

        config = GateConfig(
            host="127.0.0.1",
            tcp_port=9000,
            udp_port=9001,
            dc_id="my-dc",
            datacenter_managers=dc_managers,
            datacenter_managers_udp=dc_managers_udp,
            gate_peers=gate_peers,
            gate_peers_udp=gate_peers_udp,
            lease_timeout_seconds=60.0,
            heartbeat_timeout_seconds=45.0,
        )

        assert config.dc_id == "my-dc"
        assert len(config.datacenter_managers) == 2
        assert len(config.datacenter_managers["dc-east"]) == 2
        assert config.lease_timeout_seconds == 60.0
        assert config.heartbeat_timeout_seconds == 45.0

    def test_factory_function_with_defaults(self):
        """Factory function applies defaults correctly."""
        config = create_gate_config(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
        )

        assert config.host == "localhost"
        assert config.tcp_port == 9000
        assert config.udp_port == 9001
        assert config.dc_id == "global"
        assert config.datacenter_managers == {}
        assert config.gate_peers == []
        assert config.lease_timeout_seconds == 30.0

    def test_factory_function_with_custom_values(self):
        """Factory function applies custom values."""
        config = create_gate_config(
            host="10.0.0.1",
            tcp_port=8000,
            udp_port=8001,
            dc_id="custom-dc",
            datacenter_managers={"dc": [("10.0.1.1", 8000)]},
            gate_peers=[("10.0.2.1", 9000)],
            lease_timeout=120.0,
        )

        assert config.dc_id == "custom-dc"
        assert "dc" in config.datacenter_managers
        assert len(config.gate_peers) == 1
        assert config.lease_timeout_seconds == 120.0

    def test_config_uses_slots(self):
        """GateConfig uses slots for memory efficiency."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert hasattr(config, "__slots__")
        # Slots-based classes don't have __dict__
        assert not hasattr(config, "__dict__")


class TestGateConfigDefaults:
    """Tests for GateConfig default values."""

    def test_default_timeouts(self):
        """Verify default timeout values."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)

        assert config.lease_timeout_seconds == 30.0
        assert config.heartbeat_timeout_seconds == 30.0
        assert config.manager_dispatch_timeout_seconds == 5.0
        assert config.max_retries_per_dc == 2

    def test_default_rate_limiting(self):
        """Verify default rate limiting configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.rate_limit_inactive_cleanup_seconds == 300.0

    def test_default_latency_tracking(self):
        """Verify default latency tracking configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.latency_sample_max_age_seconds == 60.0
        assert config.latency_sample_max_count == 30

    def test_default_throughput_tracking(self):
        """Verify default throughput tracking configuration (AD-19)."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.throughput_interval_seconds == 10.0

    def test_default_orphan_tracking(self):
        """Verify default orphan tracking configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.orphan_grace_period_seconds == 120.0
        assert config.orphan_check_interval_seconds == 30.0

    def test_default_timeout_tracking(self):
        """Verify default timeout tracking configuration (AD-34)."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.timeout_check_interval_seconds == 15.0
        assert config.all_dc_stuck_threshold_seconds == 180.0

    def test_default_hash_ring(self):
        """Verify default hash ring configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.hash_ring_replicas == 150

    def test_default_forwarding(self):
        """Verify default forwarding configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.forward_timeout_seconds == 3.0
        assert config.max_forward_attempts == 3

    def test_default_stats_window(self):
        """Verify default stats window configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.stats_window_size_ms == 1000.0
        assert config.stats_drift_tolerance_ms == 100.0
        assert config.stats_max_window_age_ms == 5000.0
        assert config.stats_push_interval_ms == 1000.0

    def test_default_job_lease(self):
        """Verify default job lease configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.job_lease_duration_seconds == 300.0
        assert config.job_lease_cleanup_interval_seconds == 60.0

    def test_default_recovery(self):
        """Verify default recovery configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.recovery_max_concurrent == 3

    def test_default_circuit_breaker(self):
        """Verify default circuit breaker configuration."""
        config = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        assert config.circuit_breaker_max_errors == 5
        assert config.circuit_breaker_window_seconds == 30.0
        assert config.circuit_breaker_half_open_after_seconds == 10.0


class TestGateConfigEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_datacenter_managers(self):
        """Empty datacenter managers dict is valid."""
        config = GateConfig(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            datacenter_managers={},
        )
        assert config.datacenter_managers == {}

    def test_single_datacenter(self):
        """Single datacenter configuration."""
        config = GateConfig(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            datacenter_managers={"dc-1": [("10.0.0.1", 8000)]},
        )
        assert len(config.datacenter_managers) == 1

    def test_many_datacenters(self):
        """Many datacenters configuration."""
        dc_managers = {f"dc-{i}": [(f"10.0.{i}.1", 8000)] for i in range(20)}
        config = GateConfig(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            datacenter_managers=dc_managers,
        )
        assert len(config.datacenter_managers) == 20

    def test_many_managers_per_dc(self):
        """Many managers per datacenter."""
        managers = [(f"10.0.0.{i}", 8000) for i in range(1, 51)]
        config = GateConfig(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            datacenter_managers={"dc-1": managers},
        )
        assert len(config.datacenter_managers["dc-1"]) == 50

    def test_zero_timeouts(self):
        """Zero timeouts are valid (though not recommended)."""
        config = GateConfig(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            lease_timeout_seconds=0.0,
            heartbeat_timeout_seconds=0.0,
        )
        assert config.lease_timeout_seconds == 0.0
        assert config.heartbeat_timeout_seconds == 0.0

    def test_very_large_timeouts(self):
        """Very large timeouts are valid."""
        config = GateConfig(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            lease_timeout_seconds=3600.0,  # 1 hour
            orphan_grace_period_seconds=86400.0,  # 1 day
        )
        assert config.lease_timeout_seconds == 3600.0
        assert config.orphan_grace_period_seconds == 86400.0

    def test_special_characters_in_dc_id(self):
        """DC IDs with special characters."""
        special_ids = [
            "dc:colon",
            "dc-dash",
            "dc_underscore",
            "dc.dot",
            "dc/slash",
        ]
        for dc_id in special_ids:
            config = GateConfig(
                host="localhost",
                tcp_port=9000,
                udp_port=9001,
                dc_id=dc_id,
            )
            assert config.dc_id == dc_id

    def test_ipv6_host(self):
        """IPv6 host address."""
        config = GateConfig(
            host="::1",
            tcp_port=9000,
            udp_port=9001,
        )
        assert config.host == "::1"

    def test_port_boundaries(self):
        """Valid port numbers at boundaries."""
        # Minimum port
        config_min = GateConfig(host="localhost", tcp_port=1, udp_port=1)
        assert config_min.tcp_port == 1

        # Maximum port
        config_max = GateConfig(host="localhost", tcp_port=65535, udp_port=65535)
        assert config_max.tcp_port == 65535

    def test_factory_none_values(self):
        """Factory function handles None values correctly."""
        config = create_gate_config(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            datacenter_managers=None,
            datacenter_managers_udp=None,
            gate_peers=None,
            gate_peers_udp=None,
        )

        assert config.datacenter_managers == {}
        assert config.datacenter_managers_udp == {}
        assert config.gate_peers == []
        assert config.gate_peers_udp == []


class TestGateConfigNegativePaths:
    """Tests for invalid configurations."""

    def test_negative_port_accepted(self):
        """Negative ports are technically accepted by dataclass (no validation)."""
        # Note: Validation would happen at network bind time
        config = GateConfig(host="localhost", tcp_port=-1, udp_port=-1)
        assert config.tcp_port == -1

    def test_negative_timeout_accepted(self):
        """Negative timeouts are technically accepted (no validation)."""
        # Note: Would cause issues at runtime
        config = GateConfig(
            host="localhost",
            tcp_port=9000,
            udp_port=9001,
            lease_timeout_seconds=-1.0,
        )
        assert config.lease_timeout_seconds == -1.0


class TestGateConfigImmutability:
    """Tests for config field immutability patterns."""

    def test_field_count(self):
        """Verify expected number of configuration fields."""
        field_list = fields(GateConfig)
        # Should have all the expected configuration fields
        assert len(field_list) >= 20

    def test_config_is_dataclass(self):
        """Verify GateConfig is a proper dataclass."""
        from dataclasses import is_dataclass
        assert is_dataclass(GateConfig)

    def test_mutable_default_factories_are_safe(self):
        """Ensure mutable defaults don't share state between instances."""
        config1 = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)
        config2 = GateConfig(host="localhost", tcp_port=9000, udp_port=9001)

        # Mutate config1's dict
        config1.datacenter_managers["new-dc"] = [("10.0.0.1", 8000)]

        # config2 should not be affected
        assert "new-dc" not in config2.datacenter_managers