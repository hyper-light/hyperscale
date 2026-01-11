"""
Integration tests for OutOfBandHealthChannel (Phase 6.3).

Tests the out-of-band health channel for high-priority probes including:
- Channel start/stop lifecycle
- Probe send/receive with ACK
- Probe with NACK when overloaded
- Timeout handling
- Rate limiting
- Concurrent probes
- Edge cases and failure paths
"""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from hyperscale.distributed_rewrite.swim.health.out_of_band_health_channel import (
    OutOfBandHealthChannel,
    OOBHealthChannelConfig,
    OOBProbeResult,
    get_oob_port_for_swim_port,
    OOB_PROBE,
    OOB_ACK,
    OOB_NACK,
)


# =============================================================================
# Helper Utilities
# =============================================================================


def find_free_port() -> int:
    """Find a free port for testing."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


# =============================================================================
# Port Utility Tests
# =============================================================================


class TestPortUtility:
    """Test port calculation utility."""

    def test_get_oob_port_default_offset(self) -> None:
        """Test OOB port with default offset."""
        assert get_oob_port_for_swim_port(8000) == 8100
        assert get_oob_port_for_swim_port(9000) == 9100

    def test_get_oob_port_custom_offset(self) -> None:
        """Test OOB port with custom offset."""
        assert get_oob_port_for_swim_port(8000, offset=50) == 8050
        assert get_oob_port_for_swim_port(8000, offset=200) == 8200


# =============================================================================
# Lifecycle Tests
# =============================================================================


class TestLifecycle:
    """Test channel lifecycle management."""

    @pytest.mark.asyncio
    async def test_start_stop(self) -> None:
        """Test starting and stopping channel."""
        base_port = find_free_port()
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=base_port,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        assert channel._running is False
        assert channel._socket is None

        await channel.start()

        assert channel._running is True
        assert channel._socket is not None
        assert channel.port == base_port

        await channel.stop()

        assert channel._running is False
        assert channel._socket is None

    @pytest.mark.asyncio
    async def test_start_twice_is_safe(self) -> None:
        """Test that starting twice is idempotent."""
        base_port = find_free_port()
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=base_port,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        await channel.start()
        socket_before = channel._socket

        await channel.start()  # Should be no-op
        socket_after = channel._socket

        assert socket_before is socket_after

        await channel.stop()

    @pytest.mark.asyncio
    async def test_stop_twice_is_safe(self) -> None:
        """Test that stopping twice is safe."""
        base_port = find_free_port()
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=base_port,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        await channel.start()
        await channel.stop()
        await channel.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_port_with_offset(self) -> None:
        """Test port calculation with offset."""
        base_port = find_free_port()
        offset = 50
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=base_port,
            config=OOBHealthChannelConfig(port_offset=offset),
        )

        assert channel.port == base_port + offset

        await channel.start()
        await channel.stop()


# =============================================================================
# Probe Tests
# =============================================================================


class TestProbeSuccess:
    """Test successful probe scenarios."""

    @pytest.mark.asyncio
    async def test_probe_and_ack(self) -> None:
        """Test probe with ACK response."""
        port1 = find_free_port()
        port2 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(port_offset=0),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        try:
            await channel1.start()
            await channel2.start()

            # Give sockets time to be ready
            await asyncio.sleep(0.05)

            # Channel1 probes Channel2
            result = await channel1.probe(("127.0.0.1", port2))

            assert result.success is True
            assert result.is_overloaded is False
            assert result.error is None
            assert result.latency_ms > 0

        finally:
            await channel1.stop()
            await channel2.stop()

    @pytest.mark.asyncio
    async def test_probe_with_nack(self) -> None:
        """Test probe with NACK response when target is overloaded."""
        port1 = find_free_port()
        port2 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(port_offset=0),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        # Make channel2 report as overloaded
        channel2.set_overload_checker(lambda: True)

        try:
            await channel1.start()
            await channel2.start()
            await asyncio.sleep(0.05)

            result = await channel1.probe(("127.0.0.1", port2))

            assert result.success is True
            assert result.is_overloaded is True  # Got NACK
            assert result.error is None

        finally:
            await channel1.stop()
            await channel2.stop()


class TestProbeTimeout:
    """Test probe timeout scenarios."""

    @pytest.mark.asyncio
    async def test_probe_timeout_no_listener(self) -> None:
        """Test probe timeout when target is not listening."""
        port1 = find_free_port()
        port2 = find_free_port()  # No channel listening here

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(
                port_offset=0,
                probe_timeout_seconds=0.1,  # Short timeout for test
            ),
        )

        try:
            await channel1.start()
            await asyncio.sleep(0.05)

            result = await channel1.probe(("127.0.0.1", port2))

            assert result.success is False
            assert result.is_overloaded is False
            assert result.error == "Timeout"

        finally:
            await channel1.stop()


class TestProbeWhenNotRunning:
    """Test probing when channel is not running."""

    @pytest.mark.asyncio
    async def test_probe_before_start(self) -> None:
        """Test probe fails gracefully if channel not started."""
        port = find_free_port()
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        result = await channel.probe(("127.0.0.1", 9999))

        assert result.success is False
        assert result.error == "OOB channel not running"

    @pytest.mark.asyncio
    async def test_probe_after_stop(self) -> None:
        """Test probe fails gracefully after channel stopped."""
        port = find_free_port()
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        await channel.start()
        await channel.stop()

        result = await channel.probe(("127.0.0.1", 9999))

        assert result.success is False


# =============================================================================
# Rate Limiting Tests
# =============================================================================


class TestRateLimiting:
    """Test rate limiting for OOB channel."""

    @pytest.mark.asyncio
    async def test_per_target_cooldown(self) -> None:
        """Test per-target probe cooldown."""
        port1 = find_free_port()
        port2 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(
                port_offset=0,
                per_target_cooldown_seconds=0.5,  # Long cooldown
            ),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        try:
            await channel1.start()
            await channel2.start()
            await asyncio.sleep(0.05)

            # First probe should succeed
            result1 = await channel1.probe(("127.0.0.1", port2))
            assert result1.success is True

            # Second probe immediately should be rate limited
            result2 = await channel1.probe(("127.0.0.1", port2))
            assert result2.success is False
            assert result2.error == "Rate limited"

        finally:
            await channel1.stop()
            await channel2.stop()

    @pytest.mark.asyncio
    async def test_different_targets_not_limited(self) -> None:
        """Test that different targets are not affected by each other's cooldown."""
        port1 = find_free_port()
        port2 = find_free_port()
        port3 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(
                port_offset=0,
                per_target_cooldown_seconds=0.5,
            ),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(port_offset=0),
        )
        channel3 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port3,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        try:
            await channel1.start()
            await channel2.start()
            await channel3.start()
            await asyncio.sleep(0.05)

            # Probe target 1
            result1 = await channel1.probe(("127.0.0.1", port2))
            assert result1.success is True

            # Probe target 2 should also succeed
            result2 = await channel1.probe(("127.0.0.1", port3))
            assert result2.success is True

        finally:
            await channel1.stop()
            await channel2.stop()
            await channel3.stop()

    @pytest.mark.asyncio
    async def test_global_rate_limit(self) -> None:
        """Test global rate limit."""
        port1 = find_free_port()
        port2 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(
                port_offset=0,
                max_probes_per_second=2,  # Very low limit
                per_target_cooldown_seconds=0.0,  # No per-target limit
            ),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        try:
            await channel1.start()
            await channel2.start()
            await asyncio.sleep(0.05)

            # First 2 probes should succeed (at global limit)
            result1 = await channel1.probe(("127.0.0.1", port2))
            assert result1.success is True

            result2 = await channel1.probe(("127.0.0.1", port2))
            assert result2.success is True

            # Third should be rate limited
            result3 = await channel1.probe(("127.0.0.1", port2))
            assert result3.success is False
            assert result3.error == "Rate limited"

        finally:
            await channel1.stop()
            await channel2.stop()

    @pytest.mark.asyncio
    async def test_cleanup_stale_rate_limits(self) -> None:
        """Test cleanup of stale rate limit entries."""
        port = find_free_port()
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        # Manually add old entries
        old_time = time.monotonic() - 120.0
        channel._last_probe_time[("192.168.1.1", 8100)] = old_time
        channel._last_probe_time[("192.168.1.2", 8100)] = old_time
        channel._last_probe_time[("192.168.1.3", 8100)] = time.monotonic()

        removed = channel.cleanup_stale_rate_limits(max_age_seconds=60.0)

        assert removed == 2
        assert len(channel._last_probe_time) == 1


# =============================================================================
# Overload Checker Tests
# =============================================================================


class TestOverloadChecker:
    """Test overload checker callback."""

    @pytest.mark.asyncio
    async def test_ack_when_not_overloaded(self) -> None:
        """Test ACK sent when not overloaded."""
        port1 = find_free_port()
        port2 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(port_offset=0),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        # Not overloaded
        channel2.set_overload_checker(lambda: False)

        try:
            await channel1.start()
            await channel2.start()
            await asyncio.sleep(0.05)

            result = await channel1.probe(("127.0.0.1", port2))

            assert result.success is True
            assert result.is_overloaded is False

        finally:
            await channel1.stop()
            await channel2.stop()

    @pytest.mark.asyncio
    async def test_nack_disabled_when_configured(self) -> None:
        """Test NACK sending can be disabled."""
        port1 = find_free_port()
        port2 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(port_offset=0),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(
                port_offset=0,
                send_nack_when_overloaded=False,  # Disable NACK
            ),
        )

        # Overloaded but NACK disabled
        channel2.set_overload_checker(lambda: True)

        try:
            await channel1.start()
            await channel2.start()
            await asyncio.sleep(0.05)

            result = await channel1.probe(("127.0.0.1", port2))

            assert result.success is True
            assert result.is_overloaded is False  # Got ACK not NACK

        finally:
            await channel1.stop()
            await channel2.stop()


# =============================================================================
# Statistics Tests
# =============================================================================


class TestStatistics:
    """Test statistics tracking."""

    @pytest.mark.asyncio
    async def test_stats_after_probes(self) -> None:
        """Test statistics are tracked correctly."""
        port1 = find_free_port()
        port2 = find_free_port()

        channel1 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(port_offset=0),
        )
        channel2 = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port2,
            config=OOBHealthChannelConfig(port_offset=0),
        )

        try:
            await channel1.start()
            await channel2.start()
            await asyncio.sleep(0.05)

            # Send a probe
            await channel1.probe(("127.0.0.1", port2))

            # Wait a moment for stats to update
            await asyncio.sleep(0.05)

            stats1 = channel1.get_stats()
            stats2 = channel2.get_stats()

            assert stats1["probes_sent"] == 1
            assert stats2["probes_received"] == 1
            assert stats2["acks_sent"] == 1

        finally:
            await channel1.stop()
            await channel2.stop()


# =============================================================================
# Concurrent Probes Tests
# =============================================================================


class TestConcurrentProbes:
    """Test concurrent probe handling."""

    @pytest.mark.asyncio
    async def test_multiple_concurrent_probes(self) -> None:
        """Test multiple concurrent probes to different targets."""
        ports = [find_free_port() for _ in range(4)]

        channels = [
            OutOfBandHealthChannel(
                host="127.0.0.1",
                base_port=port,
                config=OOBHealthChannelConfig(port_offset=0),
            )
            for port in ports
        ]

        try:
            for channel in channels:
                await channel.start()
            await asyncio.sleep(0.05)

            # Channel 0 probes channels 1, 2, 3 concurrently
            probes = await asyncio.gather(
                channels[0].probe(("127.0.0.1", ports[1])),
                channels[0].probe(("127.0.0.1", ports[2])),
                channels[0].probe(("127.0.0.1", ports[3])),
            )

            # All should succeed
            for result in probes:
                assert result.success is True

        finally:
            for channel in channels:
                await channel.stop()


# =============================================================================
# OOBProbeResult Tests
# =============================================================================


class TestOOBProbeResult:
    """Test OOBProbeResult dataclass."""

    def test_result_success(self) -> None:
        """Test successful result."""
        result = OOBProbeResult(
            target=("127.0.0.1", 8100),
            success=True,
            is_overloaded=False,
            latency_ms=5.5,
        )

        assert result.success is True
        assert result.is_overloaded is False
        assert result.latency_ms == 5.5
        assert result.error is None

    def test_result_overloaded(self) -> None:
        """Test overloaded result."""
        result = OOBProbeResult(
            target=("127.0.0.1", 8100),
            success=True,
            is_overloaded=True,
            latency_ms=10.0,
        )

        assert result.success is True
        assert result.is_overloaded is True

    def test_result_failure(self) -> None:
        """Test failure result."""
        result = OOBProbeResult(
            target=("127.0.0.1", 8100),
            success=False,
            is_overloaded=False,
            latency_ms=100.0,
            error="Timeout",
        )

        assert result.success is False
        assert result.error == "Timeout"


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_probe_with_invalid_address(self) -> None:
        """Test probe to invalid address."""
        port = find_free_port()
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port,
            config=OOBHealthChannelConfig(
                port_offset=0,
                probe_timeout_seconds=0.1,
            ),
        )

        try:
            await channel.start()
            await asyncio.sleep(0.05)

            # Probe to non-existent address
            result = await channel.probe(("192.0.2.1", 9999))  # TEST-NET address

            # Should timeout or fail
            assert result.success is False

        finally:
            await channel.stop()

    def test_channel_port_property(self) -> None:
        """Test port property calculation."""
        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=8000,
            config=OOBHealthChannelConfig(port_offset=100),
        )

        assert channel.port == 8100

    @pytest.mark.asyncio
    async def test_stop_cancels_pending_probes(self) -> None:
        """Test that stopping channel cancels pending probes."""
        port1 = find_free_port()
        port2 = find_free_port()  # Not listening

        channel = OutOfBandHealthChannel(
            host="127.0.0.1",
            base_port=port1,
            config=OOBHealthChannelConfig(
                port_offset=0,
                probe_timeout_seconds=5.0,  # Long timeout
            ),
        )

        try:
            await channel.start()

            # Start a probe that will timeout
            probe_task = asyncio.create_task(
                channel.probe(("127.0.0.1", port2))
            )

            # Give it a moment
            await asyncio.sleep(0.1)

            # Stop should cancel the pending probe
            await channel.stop()

            # Probe should complete (cancelled or with error)
            result = await probe_task
            assert result.success is False

        finally:
            if channel._running:
                await channel.stop()
