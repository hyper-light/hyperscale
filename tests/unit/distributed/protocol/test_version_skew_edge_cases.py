#!/usr/bin/env python
"""
Comprehensive edge case tests for protocol version skew handling (AD-25).

Tests cover:
- Major version incompatibility rejection
- Minor version feature negotiation
- Capability negotiation edge cases
- Rolling upgrade scenarios
- Feature degradation paths
- Version boundary conditions
- Mixed cluster version scenarios
"""

import pytest

from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    FEATURE_VERSIONS,
    NegotiatedCapabilities,
    NodeCapabilities,
    ProtocolVersion,
    get_all_features,
    get_features_for_version,
    negotiate_capabilities,
)


# =============================================================================
# Test Protocol Version Compatibility
# =============================================================================


class TestMajorVersionIncompatibility:
    """Tests for major version mismatch rejection."""

    def test_reject_higher_major_version(self):
        """Node with major version 2 cannot connect to major version 1."""
        v1 = ProtocolVersion(1, 4)
        v2 = ProtocolVersion(2, 0)

        assert not v1.is_compatible_with(v2)
        assert not v2.is_compatible_with(v1)

    def test_reject_lower_major_version(self):
        """Node with major version 0 cannot connect to major version 1."""
        v0 = ProtocolVersion(0, 9)
        v1 = ProtocolVersion(1, 0)

        assert not v0.is_compatible_with(v1)
        assert not v1.is_compatible_with(v0)

    def test_reject_far_future_major_version(self):
        """Extreme version skew is rejected."""
        v1 = ProtocolVersion(1, 4)
        v100 = ProtocolVersion(100, 0)

        assert not v1.is_compatible_with(v100)

    def test_major_version_zero_special_case(self):
        """Major version 0 nodes only compatible with other 0.x nodes."""
        v0_1 = ProtocolVersion(0, 1)
        v0_9 = ProtocolVersion(0, 9)
        v1_0 = ProtocolVersion(1, 0)

        # 0.x versions compatible with each other
        assert v0_1.is_compatible_with(v0_9)
        assert v0_9.is_compatible_with(v0_1)

        # 0.x not compatible with 1.x
        assert not v0_1.is_compatible_with(v1_0)
        assert not v0_9.is_compatible_with(v1_0)


class TestMinorVersionCompatibility:
    """Tests for minor version feature negotiation."""

    def test_same_minor_version_full_compatibility(self):
        """Same minor version has full feature set."""
        v1 = ProtocolVersion(1, 4)
        v2 = ProtocolVersion(1, 4)

        assert v1.is_compatible_with(v2)

        node1 = NodeCapabilities.current()
        node2 = NodeCapabilities.current()

        result = negotiate_capabilities(node1, node2)
        assert result.compatible
        assert result.common_features == get_features_for_version(CURRENT_PROTOCOL_VERSION)

    def test_higher_minor_connects_to_lower(self):
        """Node with 1.4 can connect to 1.0 with reduced features."""
        v1_0 = ProtocolVersion(1, 0)
        v1_4 = ProtocolVersion(1, 4)

        assert v1_0.is_compatible_with(v1_4)
        assert v1_4.is_compatible_with(v1_0)

    def test_lower_minor_version_limits_features(self):
        """Features are limited to the lower version's capabilities."""
        v1_0 = ProtocolVersion(1, 0)
        v1_4 = ProtocolVersion(1, 4)

        node_old = NodeCapabilities(
            protocol_version=v1_0,
            capabilities=get_features_for_version(v1_0),
        )
        node_new = NodeCapabilities(
            protocol_version=v1_4,
            capabilities=get_features_for_version(v1_4),
        )

        result = negotiate_capabilities(node_new, node_old)

        # Should only have 1.0 features
        assert result.compatible
        assert "job_submission" in result.common_features
        assert "workflow_dispatch" in result.common_features
        assert "heartbeat" in result.common_features
        assert "cancellation" in result.common_features

        # Should NOT have 1.1+ features
        assert "batched_stats" not in result.common_features
        assert "rate_limiting" not in result.common_features
        assert "healthcheck_extensions" not in result.common_features

    def test_every_minor_version_step(self):
        """Test feature availability at each minor version."""
        version_features = {
            0: {"job_submission", "workflow_dispatch", "heartbeat", "cancellation"},
            1: {"job_submission", "workflow_dispatch", "heartbeat", "cancellation", "batched_stats", "stats_compression"},
            2: {
                "job_submission",
                "workflow_dispatch",
                "heartbeat",
                "cancellation",
                "batched_stats",
                "stats_compression",
                "client_reconnection",
                "fence_tokens",
                "idempotency_keys",
            },
            3: {
                "job_submission",
                "workflow_dispatch",
                "heartbeat",
                "cancellation",
                "batched_stats",
                "stats_compression",
                "client_reconnection",
                "fence_tokens",
                "idempotency_keys",
                "rate_limiting",
                "retry_after",
            },
            4: get_all_features(),  # All features at 1.4
        }

        for minor, expected_features in version_features.items():
            version = ProtocolVersion(1, minor)
            actual_features = get_features_for_version(version)
            assert actual_features == expected_features, f"Mismatch at version 1.{minor}"


class TestFeatureSupportChecks:
    """Tests for individual feature support checking."""

    def test_feature_exactly_at_introduction_version(self):
        """Feature is supported exactly at its introduction version."""
        # rate_limiting introduced at 1.3
        v1_3 = ProtocolVersion(1, 3)
        assert v1_3.supports_feature("rate_limiting")

        v1_2 = ProtocolVersion(1, 2)
        assert not v1_2.supports_feature("rate_limiting")

    def test_unknown_feature_not_supported(self):
        """Unknown features return False."""
        version = ProtocolVersion(1, 4)
        assert not version.supports_feature("unknown_feature")
        assert not version.supports_feature("")
        assert not version.supports_feature("future_feature_v2")

    def test_feature_supported_in_higher_major_version(self):
        """Features from major version 1 supported in major version 2."""
        # If we had a 2.x version, it should still support 1.x features
        v2_0 = ProtocolVersion(2, 0)

        # All 1.x features should be supported (major version check passes)
        assert v2_0.supports_feature("job_submission")  # 1.0 feature
        assert v2_0.supports_feature("rate_limiting")  # 1.3 feature

    def test_feature_not_supported_in_lower_major_version(self):
        """Features from major version 1 not supported in major version 0."""
        v0_9 = ProtocolVersion(0, 9)

        # 1.x features should NOT be supported
        assert not v0_9.supports_feature("job_submission")
        assert not v0_9.supports_feature("rate_limiting")


# =============================================================================
# Test Capability Negotiation Edge Cases
# =============================================================================


class TestCapabilityNegotiationEdgeCases:
    """Tests for edge cases in capability negotiation."""

    def test_negotiate_incompatible_raises_error(self):
        """Negotiating incompatible versions raises ValueError."""
        node1 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )
        node2 = NodeCapabilities(
            protocol_version=ProtocolVersion(2, 0),
            capabilities={"job_submission", "new_v2_feature"},
        )

        with pytest.raises(ValueError, match="Incompatible protocol versions"):
            node1.negotiate(node2)

    def test_negotiate_with_empty_capabilities(self):
        """Node advertising no capabilities gets no common features."""
        node1 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )
        node2 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=set(),  # No capabilities advertised
        )

        result = negotiate_capabilities(node1, node2)
        assert result.compatible
        assert result.common_features == set()  # No common features

    def test_negotiate_with_extra_unknown_capabilities(self):
        """Unknown capabilities are filtered out."""
        node1 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)) | {"experimental_feature"},
        )
        node2 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)) | {"experimental_feature"},
        )

        result = negotiate_capabilities(node1, node2)

        # experimental_feature is in both sets but not in FEATURE_VERSIONS
        # so it should be filtered out by min_version.supports_feature()
        assert "experimental_feature" not in result.common_features

    def test_negotiate_asymmetric_capabilities(self):
        """Nodes with different capability subsets negotiate intersection."""
        node1 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities={"job_submission", "heartbeat", "rate_limiting"},
        )
        node2 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities={"job_submission", "cancellation", "rate_limiting"},
        )

        result = negotiate_capabilities(node1, node2)

        assert "job_submission" in result.common_features
        assert "rate_limiting" in result.common_features
        assert "heartbeat" not in result.common_features  # Only node1 has it
        assert "cancellation" not in result.common_features  # Only node2 has it

    def test_negotiate_version_limits_capabilities(self):
        """Capabilities are limited by the lower version even if advertised."""
        # Old node advertises capabilities it doesn't actually support
        node_old = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),  # Claims all features
        )
        node_new = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )

        result = negotiate_capabilities(node_new, node_old)

        # Only 1.0 features should be enabled despite node_old's claims
        assert "job_submission" in result.common_features
        assert "rate_limiting" not in result.common_features

    def test_negotiate_returns_correct_versions(self):
        """NegotiatedCapabilities contains correct version info."""
        local = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 3),
            capabilities=get_features_for_version(ProtocolVersion(1, 3)),
        )
        remote = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )

        result = negotiate_capabilities(local, remote)

        assert result.local_version == ProtocolVersion(1, 3)
        assert result.remote_version == ProtocolVersion(1, 4)


class TestNegotiatedCapabilitiesUsage:
    """Tests for using NegotiatedCapabilities after negotiation."""

    def test_supports_method(self):
        """NegotiatedCapabilities.supports() works correctly."""
        result = NegotiatedCapabilities(
            local_version=ProtocolVersion(1, 4),
            remote_version=ProtocolVersion(1, 4),
            common_features={"job_submission", "rate_limiting"},
            compatible=True,
        )

        assert result.supports("job_submission")
        assert result.supports("rate_limiting")
        assert not result.supports("batched_stats")
        assert not result.supports("unknown")

    def test_incompatible_result_has_no_features(self):
        """Incompatible negotiation results in no common features."""
        local = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )
        remote = NodeCapabilities(
            protocol_version=ProtocolVersion(2, 0),
            capabilities={"job_submission"},
        )

        result = negotiate_capabilities(local, remote)

        assert not result.compatible
        assert result.common_features == set()
        assert not result.supports("job_submission")


# =============================================================================
# Test Rolling Upgrade Scenarios
# =============================================================================


class TestRollingUpgradeScenarios:
    """Tests simulating rolling upgrade scenarios."""

    def test_upgrade_from_1_0_to_1_4(self):
        """Simulate upgrading cluster from 1.0 to 1.4."""
        # Start: all nodes at 1.0
        v1_0_caps = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities=get_features_for_version(ProtocolVersion(1, 0)),
        )

        # End: all nodes at 1.4
        v1_4_caps = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )

        # During upgrade: mixed cluster
        # Old node connects to new node
        result = negotiate_capabilities(v1_0_caps, v1_4_caps)
        assert result.compatible
        assert result.supports("job_submission")
        assert not result.supports("rate_limiting")

        # New node connects to old node
        result = negotiate_capabilities(v1_4_caps, v1_0_caps)
        assert result.compatible
        assert result.supports("job_submission")
        assert not result.supports("rate_limiting")

    def test_incremental_minor_upgrades(self):
        """Test feature availability during incremental upgrades."""
        versions = [
            ProtocolVersion(1, 0),
            ProtocolVersion(1, 1),
            ProtocolVersion(1, 2),
            ProtocolVersion(1, 3),
            ProtocolVersion(1, 4),
        ]

        # Test all pairs during rolling upgrade
        for index in range(len(versions) - 1):
            old_version = versions[index]
            new_version = versions[index + 1]

            old_caps = NodeCapabilities(
                protocol_version=old_version,
                capabilities=get_features_for_version(old_version),
            )
            new_caps = NodeCapabilities(
                protocol_version=new_version,
                capabilities=get_features_for_version(new_version),
            )

            result = negotiate_capabilities(old_caps, new_caps)

            assert result.compatible, f"{old_version} should be compatible with {new_version}"
            # Common features should be limited to old version
            assert result.common_features == get_features_for_version(old_version)

    def test_major_version_upgrade_rejection(self):
        """Major version upgrade requires cluster restart (no rolling upgrade)."""
        v1_4 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )
        v2_0 = NodeCapabilities(
            protocol_version=ProtocolVersion(2, 0),
            capabilities={"job_submission_v2"},  # New v2 features
        )

        result = negotiate_capabilities(v1_4, v2_0)
        assert not result.compatible
        assert not result.supports("job_submission")


class TestFeatureDegradation:
    """Tests for graceful feature degradation."""

    def test_degrade_without_rate_limiting(self):
        """System operates without rate limiting for older nodes."""
        new_node = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )
        old_node = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities=get_features_for_version(ProtocolVersion(1, 2)),
        )

        result = negotiate_capabilities(new_node, old_node)

        # Can still do basic operations
        assert result.supports("job_submission")
        assert result.supports("workflow_dispatch")
        assert result.supports("fence_tokens")

        # Cannot use rate limiting
        assert not result.supports("rate_limiting")
        assert not result.supports("retry_after")

    def test_degrade_to_minimal_features(self):
        """Degradation to 1.0 still allows basic operation."""
        new_node = NodeCapabilities.current()
        old_node = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities=get_features_for_version(ProtocolVersion(1, 0)),
        )

        result = negotiate_capabilities(new_node, old_node)

        # Basic workflow functionality works
        assert result.supports("job_submission")
        assert result.supports("workflow_dispatch")
        assert result.supports("heartbeat")
        assert result.supports("cancellation")

    def test_feature_check_before_use(self):
        """Pattern for checking feature before use."""
        result = NegotiatedCapabilities(
            local_version=ProtocolVersion(1, 4),
            remote_version=ProtocolVersion(1, 2),
            common_features=get_features_for_version(ProtocolVersion(1, 2)),
            compatible=True,
        )

        # Pattern: check before use
        if result.supports("rate_limiting"):
            # Use rate limiting features
            pass
        else:
            # Fall back to non-rate-limited behavior
            pass

        # Verify the pattern works
        assert not result.supports("rate_limiting")
        assert result.supports("fence_tokens")


# =============================================================================
# Test Version Boundary Conditions
# =============================================================================


class TestVersionBoundaryConditions:
    """Tests for edge cases at version boundaries."""

    def test_version_zero_zero(self):
        """Version 0.0 is valid but has no features."""
        v0_0 = ProtocolVersion(0, 0)
        features = get_features_for_version(v0_0)
        assert features == set()

    def test_very_high_minor_version(self):
        """High minor version works correctly."""
        v1_999 = ProtocolVersion(1, 999)

        # Should support all 1.x features
        assert v1_999.supports_feature("job_submission")
        assert v1_999.supports_feature("rate_limiting")
        assert v1_999.supports_feature("healthcheck_extensions")

    def test_version_string_representation(self):
        """Version string formatting is correct."""
        v1_4 = ProtocolVersion(1, 4)
        assert str(v1_4) == "1.4"
        assert repr(v1_4) == "ProtocolVersion(1, 4)"

    def test_version_equality(self):
        """Version equality and hashing work correctly."""
        v1 = ProtocolVersion(1, 4)
        v2 = ProtocolVersion(1, 4)
        v3 = ProtocolVersion(1, 3)

        assert v1 == v2
        assert v1 != v3
        assert hash(v1) == hash(v2)

        # Can use in sets/dicts
        version_set = {v1, v2, v3}
        assert len(version_set) == 2

    def test_version_immutability(self):
        """ProtocolVersion is immutable (frozen dataclass)."""
        v = ProtocolVersion(1, 4)

        with pytest.raises(AttributeError):
            v.major = 2  # type: ignore

        with pytest.raises(AttributeError):
            v.minor = 5  # type: ignore


# =============================================================================
# Test Mixed Cluster Scenarios
# =============================================================================


class TestMixedClusterScenarios:
    """Tests simulating clusters with multiple version combinations."""

    def test_three_node_cluster_mixed_versions(self):
        """Three nodes with different versions negotiate correctly."""
        node_1_0 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities=get_features_for_version(ProtocolVersion(1, 0)),
        )
        node_1_2 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities=get_features_for_version(ProtocolVersion(1, 2)),
        )
        node_1_4 = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )

        # All pairs should be compatible
        pairs = [
            (node_1_0, node_1_2),
            (node_1_0, node_1_4),
            (node_1_2, node_1_4),
        ]

        for node_a, node_b in pairs:
            result = negotiate_capabilities(node_a, node_b)
            assert result.compatible

    def test_find_minimum_cluster_capabilities(self):
        """Find common capabilities across entire cluster."""
        nodes = [
            NodeCapabilities(
                protocol_version=ProtocolVersion(1, 0),
                capabilities=get_features_for_version(ProtocolVersion(1, 0)),
            ),
            NodeCapabilities(
                protocol_version=ProtocolVersion(1, 2),
                capabilities=get_features_for_version(ProtocolVersion(1, 2)),
            ),
            NodeCapabilities(
                protocol_version=ProtocolVersion(1, 4),
                capabilities=get_features_for_version(ProtocolVersion(1, 4)),
            ),
        ]

        # Find intersection of all capabilities
        common = nodes[0].capabilities.copy()
        min_version = nodes[0].protocol_version

        for node in nodes[1:]:
            common &= node.capabilities
            if node.protocol_version.minor < min_version.minor:
                min_version = node.protocol_version

        # Common features should be 1.0 features
        expected = get_features_for_version(ProtocolVersion(1, 0))
        assert common == expected

    def test_cluster_with_incompatible_node(self):
        """Detect and handle incompatible node in cluster."""
        nodes = [
            NodeCapabilities(
                protocol_version=ProtocolVersion(1, 4),
                capabilities=get_features_for_version(ProtocolVersion(1, 4)),
            ),
            NodeCapabilities(
                protocol_version=ProtocolVersion(1, 4),
                capabilities=get_features_for_version(ProtocolVersion(1, 4)),
            ),
            NodeCapabilities(
                protocol_version=ProtocolVersion(2, 0),  # Incompatible!
                capabilities={"new_v2_feature"},
            ),
        ]

        reference = nodes[0]
        incompatible_nodes = []

        for index, node in enumerate(nodes):
            result = negotiate_capabilities(reference, node)
            if not result.compatible:
                incompatible_nodes.append(index)

        assert incompatible_nodes == [2]


# =============================================================================
# Test Current Version Factory
# =============================================================================


class TestCurrentVersionFactory:
    """Tests for NodeCapabilities.current() factory."""

    def test_current_has_all_features(self):
        """NodeCapabilities.current() has all current features."""
        caps = NodeCapabilities.current()

        assert caps.protocol_version == CURRENT_PROTOCOL_VERSION
        assert caps.capabilities == get_features_for_version(CURRENT_PROTOCOL_VERSION)

    def test_current_with_node_version(self):
        """NodeCapabilities.current() can include node version."""
        caps = NodeCapabilities.current(node_version="hyperscale-1.2.3")

        assert caps.node_version == "hyperscale-1.2.3"
        assert caps.protocol_version == CURRENT_PROTOCOL_VERSION

    def test_current_is_self_compatible(self):
        """Two current nodes are fully compatible."""
        caps1 = NodeCapabilities.current()
        caps2 = NodeCapabilities.current()

        result = negotiate_capabilities(caps1, caps2)

        assert result.compatible
        assert result.common_features == caps1.capabilities


# =============================================================================
# Test Feature Version Map Integrity
# =============================================================================


class TestFeatureVersionMapIntegrity:
    """Tests for FEATURE_VERSIONS map consistency."""

    def test_all_features_have_valid_versions(self):
        """All features map to valid ProtocolVersion objects."""
        for feature, version in FEATURE_VERSIONS.items():
            assert isinstance(feature, str)
            assert isinstance(version, ProtocolVersion)
            assert version.major >= 0
            assert version.minor >= 0

    def test_no_features_above_current_version(self):
        """No feature requires a version higher than CURRENT_PROTOCOL_VERSION."""
        for feature, version in FEATURE_VERSIONS.items():
            assert (
                version.major < CURRENT_PROTOCOL_VERSION.major
                or (
                    version.major == CURRENT_PROTOCOL_VERSION.major
                    and version.minor <= CURRENT_PROTOCOL_VERSION.minor
                )
            ), f"Feature {feature} requires {version}, but current is {CURRENT_PROTOCOL_VERSION}"

    def test_base_features_at_1_0(self):
        """Essential features are available at version 1.0."""
        base_features = {"job_submission", "workflow_dispatch", "heartbeat", "cancellation"}

        for feature in base_features:
            version = FEATURE_VERSIONS[feature]
            assert version == ProtocolVersion(1, 0), f"{feature} should be at 1.0"

    def test_get_all_features_matches_map(self):
        """get_all_features() returns exactly FEATURE_VERSIONS keys."""
        assert get_all_features() == set(FEATURE_VERSIONS.keys())
