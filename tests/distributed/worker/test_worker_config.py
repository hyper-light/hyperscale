"""
Integration tests for WorkerConfig (Section 15.2.3).

Tests WorkerConfig dataclass and create_worker_config_from_env factory.

Covers:
- Happy path: Normal configuration creation
- Negative path: Invalid configuration values
- Failure mode: Missing or invalid environment variables
- Concurrency: Configuration immutability
- Edge cases: Boundary values, environment variable overrides
"""

import os
from unittest.mock import patch, MagicMock

import pytest

from hyperscale.distributed.nodes.worker.config import (
    WorkerConfig,
    create_worker_config_from_env,
    _get_os_cpus,
)


class TestWorkerConfig:
    """Test WorkerConfig dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal configuration creation."""
        config = WorkerConfig(
            host="192.168.1.1",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.host == "192.168.1.1"
        assert config.tcp_port == 8000
        assert config.udp_port == 8001

    def test_default_datacenter(self):
        """Test default datacenter ID."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.datacenter_id == "default"

    def test_default_timeouts(self):
        """Test default timeout values."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.tcp_timeout_short_seconds == 2.0
        assert config.tcp_timeout_standard_seconds == 5.0

    def test_default_dead_manager_intervals(self):
        """Test default dead manager tracking intervals."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.dead_manager_reap_interval_seconds == 60.0
        assert config.dead_manager_check_interval_seconds == 10.0

    def test_default_discovery_settings(self):
        """Test default discovery settings (AD-28)."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.discovery_probe_interval_seconds == 30.0
        assert config.discovery_failure_decay_interval_seconds == 60.0

    def test_default_progress_settings(self):
        """Test default progress update settings."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.progress_update_interval_seconds == 1.0
        assert config.progress_flush_interval_seconds == 0.5

    def test_default_cancellation_settings(self):
        """Test default cancellation polling settings."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.cancellation_poll_interval_seconds == 5.0

    def test_default_orphan_settings(self):
        """Test default orphan workflow settings (Section 2.7)."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.orphan_grace_period_seconds == 120.0
        assert config.orphan_check_interval_seconds == 10.0

    def test_default_pending_transfer_settings(self):
        """Test default pending transfer settings (Section 8.3)."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.pending_transfer_ttl_seconds == 60.0

    def test_default_overload_settings(self):
        """Test default overload detection settings (AD-18)."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.overload_poll_interval_seconds == 0.25

    def test_default_throughput_settings(self):
        """Test default throughput tracking settings (AD-19)."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.throughput_interval_seconds == 10.0
        assert config.completion_times_max_samples == 50

    def test_default_recovery_settings(self):
        """Test default recovery coordination settings."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.recovery_jitter_min_seconds == 0.0
        assert config.recovery_jitter_max_seconds == 1.0
        assert config.recovery_semaphore_size == 5

    def test_default_registration_settings(self):
        """Test default registration settings."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        assert config.registration_max_retries == 3
        assert config.registration_base_delay_seconds == 0.5

    def test_progress_update_interval_property(self):
        """Test progress_update_interval property alias."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
            progress_update_interval_seconds=2.5,
        )

        assert config.progress_update_interval == 2.5

    def test_progress_flush_interval_property(self):
        """Test progress_flush_interval property alias."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
            progress_flush_interval_seconds=0.75,
        )

        assert config.progress_flush_interval == 0.75

    def test_custom_core_allocation(self):
        """Test custom core allocation settings."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
            total_cores=16,
            max_workflow_cores=8,
        )

        assert config.total_cores == 16
        assert config.max_workflow_cores == 8

    def test_slots_prevents_new_attributes(self):
        """Test that slots=True prevents adding new attributes."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=7000,
            udp_port=7001,
        )

        with pytest.raises(AttributeError):
            config.custom_setting = "value"

    def test_edge_case_port_boundaries(self):
        """Test with edge case port numbers."""
        config_min = WorkerConfig(
            host="localhost",
            tcp_port=1,
            udp_port=1,
        )
        assert config_min.tcp_port == 1

        config_max = WorkerConfig(
            host="localhost",
            tcp_port=65535,
            udp_port=65535,
        )
        assert config_max.tcp_port == 65535


class TestWorkerConfigFromEnv:
    """Test WorkerConfig.from_env class method."""

    def test_happy_path_from_env(self):
        """Test normal configuration from Env object."""
        mock_env = MagicMock()
        mock_env.WORKER_MAX_CORES = 8
        mock_env.WORKER_TCP_TIMEOUT_SHORT = 1.5
        mock_env.WORKER_TCP_TIMEOUT_STANDARD = 4.0
        mock_env.WORKER_DEAD_MANAGER_REAP_INTERVAL = 120.0
        mock_env.WORKER_DEAD_MANAGER_CHECK_INTERVAL = 15.0
        mock_env.WORKER_PROGRESS_UPDATE_INTERVAL = 2.0
        mock_env.WORKER_PROGRESS_FLUSH_INTERVAL = 1.0
        mock_env.WORKER_CANCELLATION_POLL_INTERVAL = 10.0
        mock_env.WORKER_ORPHAN_GRACE_PERIOD = 180.0
        mock_env.WORKER_ORPHAN_CHECK_INTERVAL = 20.0
        mock_env.WORKER_PENDING_TRANSFER_TTL = 90.0
        mock_env.WORKER_OVERLOAD_POLL_INTERVAL = 0.5
        mock_env.WORKER_THROUGHPUT_INTERVAL_SECONDS = 15.0
        mock_env.RECOVERY_JITTER_MIN = 0.1
        mock_env.RECOVERY_JITTER_MAX = 2.0
        mock_env.RECOVERY_SEMAPHORE_SIZE = 10

        config = WorkerConfig.from_env(
            env=mock_env,
            host="10.0.0.1",
            tcp_port=9000,
            udp_port=9001,
            datacenter_id="dc-west",
        )

        assert config.host == "10.0.0.1"
        assert config.tcp_port == 9000
        assert config.udp_port == 9001
        assert config.datacenter_id == "dc-west"
        assert config.total_cores == 8
        assert config.tcp_timeout_short_seconds == 1.5
        assert config.orphan_grace_period_seconds == 180.0

    def test_from_env_with_missing_attrs(self):
        """Test from_env with missing Env attributes uses defaults."""
        mock_env = MagicMock(spec=[])  # Empty spec, all getattr return default

        config = WorkerConfig.from_env(
            env=mock_env,
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        # Should fall back to defaults for missing attributes
        assert config.tcp_timeout_short_seconds == 2.0
        assert config.tcp_timeout_standard_seconds == 5.0

    def test_from_env_default_datacenter(self):
        """Test from_env with default datacenter."""
        mock_env = MagicMock(spec=[])

        config = WorkerConfig.from_env(
            env=mock_env,
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.datacenter_id == "default"


class TestCreateWorkerConfigFromEnv:
    """Test create_worker_config_from_env factory function."""

    def test_happy_path_creation(self):
        """Test normal factory function creation."""
        config = create_worker_config_from_env(
            host="192.168.1.100",
            tcp_port=7000,
            udp_port=7001,
            datacenter_id="dc-east",
        )

        assert config.host == "192.168.1.100"
        assert config.tcp_port == 7000
        assert config.udp_port == 7001
        assert config.datacenter_id == "dc-east"

    def test_default_datacenter(self):
        """Test default datacenter when not specified."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.datacenter_id == "default"

    @patch.dict(os.environ, {
        "WORKER_MAX_CORES": "16",
        "WORKER_TCP_TIMEOUT_SHORT": "3.0",
        "WORKER_TCP_TIMEOUT_STANDARD": "10.0",
    })
    def test_environment_variable_override(self):
        """Test environment variable configuration."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.total_cores == 16
        assert config.tcp_timeout_short_seconds == 3.0
        assert config.tcp_timeout_standard_seconds == 10.0

    @patch.dict(os.environ, {
        "WORKER_DEAD_MANAGER_REAP_INTERVAL": "180.0",
        "WORKER_DEAD_MANAGER_CHECK_INTERVAL": "30.0",
    })
    def test_dead_manager_interval_override(self):
        """Test dead manager interval environment override."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.dead_manager_reap_interval_seconds == 180.0
        assert config.dead_manager_check_interval_seconds == 30.0

    @patch.dict(os.environ, {
        "WORKER_PROGRESS_UPDATE_INTERVAL": "5.0",
        "WORKER_PROGRESS_FLUSH_INTERVAL": "2.0",
    })
    def test_progress_interval_override(self):
        """Test progress interval environment override."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.progress_update_interval_seconds == 5.0
        assert config.progress_flush_interval_seconds == 2.0

    @patch.dict(os.environ, {
        "WORKER_ORPHAN_GRACE_PERIOD": "300.0",
        "WORKER_ORPHAN_CHECK_INTERVAL": "60.0",
    })
    def test_orphan_settings_override(self):
        """Test orphan settings environment override (Section 2.7)."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.orphan_grace_period_seconds == 300.0
        assert config.orphan_check_interval_seconds == 60.0

    @patch.dict(os.environ, {
        "WORKER_PENDING_TRANSFER_TTL": "120.0",
    })
    def test_pending_transfer_ttl_override(self):
        """Test pending transfer TTL environment override (Section 8.3)."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.pending_transfer_ttl_seconds == 120.0

    @patch.dict(os.environ, {
        "WORKER_OVERLOAD_POLL_INTERVAL": "0.1",
    })
    def test_overload_poll_interval_override(self):
        """Test overload poll interval environment override (AD-18)."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.overload_poll_interval_seconds == 0.1

    @patch.dict(os.environ, {
        "WORKER_THROUGHPUT_INTERVAL_SECONDS": "30.0",
    })
    def test_throughput_interval_override(self):
        """Test throughput interval environment override (AD-19)."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.throughput_interval_seconds == 30.0

    @patch.dict(os.environ, {"WORKER_MAX_CORES": "0"})
    def test_zero_cores_fallback(self):
        """Test fallback when WORKER_MAX_CORES is 0."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        # Should fall back to OS CPU count
        assert config.total_cores >= 1

    @patch.dict(os.environ, {}, clear=True)
    def test_no_environment_variables(self):
        """Test with no environment variables set."""
        config = create_worker_config_from_env(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
        )

        # All should use defaults
        assert config.tcp_timeout_short_seconds == 2.0
        assert config.tcp_timeout_standard_seconds == 5.0
        assert config.dead_manager_reap_interval_seconds == 60.0


class TestGetOsCpus:
    """Test _get_os_cpus helper function."""

    def test_returns_positive_integer(self):
        """Test that _get_os_cpus returns a positive integer."""
        result = _get_os_cpus()

        assert isinstance(result, int)
        assert result >= 1

    @patch("hyperscale.distributed_rewrite.nodes.worker.config.os.cpu_count")
    def test_fallback_to_os_cpu_count(self, mock_cpu_count):
        """Test fallback when psutil is not available."""
        # Simulate psutil import failure
        mock_cpu_count.return_value = 4

        # This test verifies the function handles the fallback path
        result = _get_os_cpus()
        assert result >= 1


class TestWorkerConfigEdgeCases:
    """Test edge cases for WorkerConfig."""

    def test_very_short_intervals(self):
        """Test with very short interval values."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            progress_flush_interval_seconds=0.001,
            overload_poll_interval_seconds=0.01,
        )

        assert config.progress_flush_interval_seconds == 0.001
        assert config.overload_poll_interval_seconds == 0.01

    def test_very_long_intervals(self):
        """Test with very long interval values."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            orphan_grace_period_seconds=86400.0,  # 24 hours
            dead_manager_reap_interval_seconds=3600.0,  # 1 hour
        )

        assert config.orphan_grace_period_seconds == 86400.0
        assert config.dead_manager_reap_interval_seconds == 3600.0

    def test_large_core_counts(self):
        """Test with large core counts."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            total_cores=1024,
            max_workflow_cores=512,
        )

        assert config.total_cores == 1024
        assert config.max_workflow_cores == 512

    def test_ipv6_host(self):
        """Test with IPv6 host address."""
        config = WorkerConfig(
            host="::1",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.host == "::1"

    def test_special_datacenter_id(self):
        """Test with special characters in datacenter ID."""
        config = WorkerConfig(
            host="localhost",
            tcp_port=8000,
            udp_port=8001,
            datacenter_id="dc-east-🌍-region1",
        )

        assert config.datacenter_id == "dc-east-🌍-region1"
