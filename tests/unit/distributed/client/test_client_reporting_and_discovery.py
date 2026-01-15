"""
Integration tests for ClientReportingManager and ClientDiscovery (Sections 15.1.9, 15.1.10).

Tests ClientReportingManager for local file-based reporting and ClientDiscovery
for ping, workflow query, and datacenter discovery operations.

Covers:
- Happy path: Normal reporting and discovery operations
- Negative path: Invalid inputs, missing configurations
- Failure mode: Reporter failures, network errors, timeouts
- Concurrency: Concurrent operations
- Edge cases: Empty results, special characters, many targets
"""

import asyncio
import secrets
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hyperscale.distributed.nodes.client.reporting import ClientReportingManager
from hyperscale.distributed.nodes.client.discovery import ClientDiscovery
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.targets import ClientTargetSelector
from hyperscale.distributed.models import (
    ManagerPingResponse,
    GatePingResponse,
    WorkflowQueryResponse,
    WorkflowStatusInfo,
    GateWorkflowQueryResponse,
    DatacenterWorkflowStatus,
    DatacenterListResponse,
    DatacenterInfo,
)
from hyperscale.reporting.json import JSONConfig
from hyperscale.reporting.csv import CSVConfig
from hyperscale.logging import Logger


# =============================================================================
# ClientReportingManager Tests
# =============================================================================


class TestClientReportingManager:
    """Test ClientReportingManager for local file-based reporting."""

    @pytest.fixture
    def state(self):
        """Create ClientState instance."""
        return ClientState()

    @pytest.fixture
    def config(self):
        """Create ClientConfig instance."""
        return ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("manager1", 7000)],
            gates=[("gate1", 9000)],
        )

    @pytest.fixture
    def logger(self):
        """Create mock logger."""
        mock_logger = MagicMock(spec=Logger)
        mock_logger.log = AsyncMock()
        return mock_logger

    @pytest.fixture
    def reporting_manager(self, state, config, logger):
        """Create ClientReportingManager instance."""
        return ClientReportingManager(state, config, logger)

    @pytest.mark.asyncio
    async def test_happy_path_with_default_json_config(self, reporting_manager):
        """Test submission with default JSON config creation."""
        job_id = "job-123"
        workflow_name = "MyWorkflow"
        workflow_stats = {"total": 100, "success": 95}

        # Mock Reporter
        with patch("hyperscale.distributed.nodes.client.reporting.Reporter") as mock_reporter_class:
            mock_reporter = AsyncMock()
            mock_reporter_class.return_value = mock_reporter

            await reporting_manager.submit_to_local_reporters(
                job_id, workflow_name, workflow_stats
            )

            # Should create default JSON config and submit
            assert mock_reporter_class.call_count == 1
            created_config = mock_reporter_class.call_args[0][0]
            assert isinstance(created_config, JSONConfig)
            assert created_config.workflow_results_filepath == "myworkflow_workflow_results.json"
            assert created_config.step_results_filepath == "myworkflow_step_results.json"

            # Should call connect, submit workflow/step, and close
            mock_reporter.connect.assert_called_once()
            mock_reporter.submit_workflow_results.assert_called_once_with(workflow_stats)
            mock_reporter.submit_step_results.assert_called_once_with(workflow_stats)
            mock_reporter.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_with_provided_configs(self, reporting_manager, state):
        """Test submission with user-provided reporter configs."""
        job_id = "job-456"
        workflow_name = "TestWorkflow"
        workflow_stats = {"total": 50}

        # Add reporter configs to state
        json_config = JSONConfig(
            workflow_results_filepath="custom_workflow.json",
            step_results_filepath="custom_step.json",
        )
        csv_config = CSVConfig(
            workflow_results_filepath="custom_workflow.csv",
            step_results_filepath="custom_step.csv",
        )
        state._job_reporting_configs[job_id] = [json_config, csv_config]

        with patch("hyperscale.distributed.nodes.client.reporting.Reporter") as mock_reporter_class:
            mock_reporter = AsyncMock()
            mock_reporter_class.return_value = mock_reporter

            await reporting_manager.submit_to_local_reporters(
                job_id, workflow_name, workflow_stats
            )

            # Should use provided configs, create 2 reporters
            assert mock_reporter_class.call_count == 2
            assert mock_reporter.connect.call_count == 2
            assert mock_reporter.close.call_count == 2

    @pytest.mark.asyncio
    async def test_reporter_failure_does_not_raise(self, reporting_manager):
        """Test that reporter failures are silently caught (best-effort)."""
        job_id = "job-fail"
        workflow_name = "FailWorkflow"
        workflow_stats = {"total": 10}

        with patch("hyperscale.distributed.nodes.client.reporting.Reporter") as mock_reporter_class:
            # Make reporter raise exception on connect
            mock_reporter = AsyncMock()
            mock_reporter.connect.side_effect = Exception("Connection failed")
            mock_reporter_class.return_value = mock_reporter

            # Should not raise - best effort submission
            await reporting_manager.submit_to_local_reporters(
                job_id, workflow_name, workflow_stats
            )

            # Reporter was created but failed
            assert mock_reporter_class.call_count == 1

    @pytest.mark.asyncio
    async def test_reporter_submit_failure_does_not_raise(self, reporting_manager):
        """Test that submit failures are caught and reporter still closes."""
        job_id = "job-submit-fail"
        workflow_name = "SubmitFailWorkflow"
        workflow_stats = {"total": 5}

        with patch("hyperscale.distributed.nodes.client.reporting.Reporter") as mock_reporter_class:
            mock_reporter = AsyncMock()
            mock_reporter.submit_workflow_results.side_effect = Exception("Submit failed")
            mock_reporter_class.return_value = mock_reporter

            # Should not raise
            await reporting_manager.submit_to_local_reporters(
                job_id, workflow_name, workflow_stats
            )

            # Should still call close despite submit failure
            mock_reporter.close.assert_called_once()

    def test_get_local_reporter_configs_filters_correctly(self, reporting_manager, state, config):
        """Test filtering to only local file-based reporters."""
        job_id = "job-filter"

        # Mix of local and non-local configs
        json_config = JSONConfig(
            workflow_results_filepath="test.json",
            step_results_filepath="test_step.json",
        )
        csv_config = CSVConfig(
            workflow_results_filepath="test.csv",
            step_results_filepath="test_step.csv",
        )
        # Mock non-local config (e.g., database reporter)
        db_config = MagicMock()
        db_config.reporter_type = MagicMock()
        db_config.reporter_type.name = "postgres"

        state._job_reporting_configs[job_id] = [json_config, csv_config, db_config]

        local_configs = reporting_manager._get_local_reporter_configs(job_id)

        # Should filter to only JSON and CSV (default local_reporter_types)
        assert len(local_configs) == 2
        assert json_config in local_configs
        assert csv_config in local_configs
        assert db_config not in local_configs

    def test_get_local_reporter_configs_no_configs(self, reporting_manager):
        """Test getting configs for job with none configured."""
        job_id = "job-no-configs"

        local_configs = reporting_manager._get_local_reporter_configs(job_id)

        assert local_configs == []

    def test_create_default_reporter_configs(self, reporting_manager):
        """Test default JSON config creation."""
        workflow_name = "TestWorkflow"

        configs = reporting_manager._create_default_reporter_configs(workflow_name)

        assert len(configs) == 1
        assert isinstance(configs[0], JSONConfig)
        assert configs[0].workflow_results_filepath == "testworkflow_workflow_results.json"
        assert configs[0].step_results_filepath == "testworkflow_step_results.json"

    @pytest.mark.asyncio
    async def test_concurrent_submissions(self, reporting_manager):
        """Test concurrent submissions to multiple reporters."""
        job_ids = [f"job-{i}" for i in range(10)]
        workflow_stats = {"total": 100}

        async def submit_one(job_id):
            await reporting_manager.submit_to_local_reporters(
                job_id, "ConcurrentWorkflow", workflow_stats
            )

        with patch("hyperscale.distributed.nodes.client.reporting.Reporter") as mock_reporter_class:
            mock_reporter = AsyncMock()
            mock_reporter_class.return_value = mock_reporter

            await asyncio.gather(*[submit_one(jid) for jid in job_ids])

            # Should create 10 reporters (one per job)
            assert mock_reporter_class.call_count == 10

    def test_edge_case_special_characters_in_workflow_name(self, reporting_manager):
        """Test workflow names with special characters."""
        workflow_name = "Test-Workflow_123-🚀"

        configs = reporting_manager._create_default_reporter_configs(workflow_name)

        # Should lowercase and use as-is
        assert configs[0].workflow_results_filepath == "test-workflow_123-🚀_workflow_results.json"

    def test_edge_case_very_long_workflow_name(self, reporting_manager):
        """Test with extremely long workflow name."""
        long_workflow_name = "Workflow" + "X" * 1000

        configs = reporting_manager._create_default_reporter_configs(long_workflow_name)

        # Should create config without error
        assert len(configs) == 1
        assert long_workflow_name.lower() in configs[0].workflow_results_filepath

    @pytest.mark.asyncio
    async def test_edge_case_empty_workflow_stats(self, reporting_manager):
        """Test submission with empty stats dictionary."""
        job_id = "job-empty-stats"
        workflow_name = "EmptyStatsWorkflow"
        workflow_stats = {}

        with patch("hyperscale.distributed.nodes.client.reporting.Reporter") as mock_reporter_class:
            mock_reporter = AsyncMock()
            mock_reporter_class.return_value = mock_reporter

            await reporting_manager.submit_to_local_reporters(
                job_id, workflow_name, workflow_stats
            )

            # Should still submit empty dict
            mock_reporter.submit_workflow_results.assert_called_once_with({})


# =============================================================================
# ClientDiscovery Tests
# =============================================================================


class TestClientDiscovery:
    """Test ClientDiscovery for ping, query, and datacenter discovery."""

    @pytest.fixture
    def state(self):
        """Create ClientState instance."""
        return ClientState()

    @pytest.fixture
    def config(self):
        """Create ClientConfig instance."""
        return ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("manager1", 7000), ("manager2", 7001)],
            gates=[("gate1", 9000), ("gate2", 9001)],
        )

    @pytest.fixture
    def logger(self):
        """Create mock logger."""
        mock_logger = MagicMock(spec=Logger)
        mock_logger.log = AsyncMock()
        return mock_logger

    @pytest.fixture
    def targets(self, config, state):
        """Create ClientTargetSelector instance."""
        return ClientTargetSelector(config, state)

    @pytest.fixture
    def send_tcp(self):
        """Create mock send_tcp function."""
        return AsyncMock()

    @pytest.fixture
    def discovery(self, state, config, logger, targets, send_tcp):
        """Create ClientDiscovery instance."""
        return ClientDiscovery(state, config, logger, targets, send_tcp)

    # =========================================================================
    # Ping Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_happy_path_ping_manager(self, discovery, send_tcp):
        """Test successful manager ping."""
        ping_response = ManagerPingResponse(
                request_id="req-123",
                manager_id="mgr-1",
                datacenter="dc-east",
                host="localhost",
                port=7000,
                is_leader=True,
                state="healthy",
                term=1,
                worker_count=5,
                active_job_count=10,
            )
        send_tcp.return_value = (ping_response.dump(), None)

        result = await discovery.ping_manager(("manager1", 7000))

        assert result.manager_id == "mgr-1"
        assert result.state == "healthy"
        assert result.worker_count == 5
        send_tcp.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_ping_gate(self, discovery, send_tcp):
        """Test successful gate ping."""
        ping_response = GatePingResponse(
                request_id="req-456",
                gate_id="gate-1",
                datacenter="dc-1",
                host="localhost",
                port=9000,
                is_leader=True,
                state="healthy",
                term=1,
                active_datacenter_count=3,
                active_job_count=50,
            )
        send_tcp.return_value = (ping_response.dump(), None)

        result = await discovery.ping_gate(("gate1", 9000))

        assert result.gate_id == "gate-1"
        assert result.state == "healthy"
        assert result.active_datacenter_count == 3

    @pytest.mark.asyncio
    async def test_ping_manager_no_targets_configured(self, state, logger, send_tcp):
        """Test ping_manager with no managers configured."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],  # No managers
            gates=[],
        )
        targets = ClientTargetSelector(config, state)
        discovery = ClientDiscovery(state, config, logger, targets, send_tcp)

        with pytest.raises(RuntimeError, match="No managers configured"):
            await discovery.ping_manager()

    @pytest.mark.asyncio
    async def test_ping_gate_no_targets_configured(self, state, logger, send_tcp):
        """Test ping_gate with no gates configured."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[],  # No gates
        )
        targets = ClientTargetSelector(config, state)
        discovery = ClientDiscovery(state, config, logger, targets, send_tcp)

        with pytest.raises(RuntimeError, match="No gates configured"):
            await discovery.ping_gate()

    @pytest.mark.asyncio
    async def test_ping_manager_server_error(self, discovery, send_tcp):
        """Test ping when server returns error."""
        send_tcp.return_value = (b'error', None)

        with pytest.raises(RuntimeError, match="Ping failed: server returned error"):
            await discovery.ping_manager(("manager1", 7000))

    @pytest.mark.asyncio
    async def test_ping_manager_network_exception(self, discovery, send_tcp):
        """Test ping when network exception occurs."""
        send_tcp.return_value = (ConnectionError("Network down"), None)

        with pytest.raises(RuntimeError, match="Ping failed"):
            await discovery.ping_manager(("manager1", 7000))

    @pytest.mark.asyncio
    async def test_ping_all_managers_success(self, discovery, send_tcp):
        """Test pinging all managers concurrently."""
        # Mock responses for both managers
        async def mock_send(target, msg_type, data, timeout):
            if target[1] == 7000:
                response = ManagerPingResponse(
                request_id="req-1",
                manager_id="mgr-1",
                datacenter="dc-east",
                host="localhost",
                port=7000,
                is_leader=True,
                state="healthy",
                term=1,
                worker_count=3,
                active_job_count=5,
            )
            else:
                response = ManagerPingResponse(
                request_id="req-2",
                manager_id="mgr-2",
                datacenter="dc-west",
                host="localhost",
                port=7000,
                is_leader=True,
                state="healthy",
                term=1,
                worker_count=4,
                active_job_count=8,
            )
            return (response.dump(), None)

        send_tcp.side_effect = mock_send

        results = await discovery.ping_all_managers()

        assert len(results) == 2
        assert ("manager1", 7000) in results
        assert ("manager2", 7001) in results
        assert isinstance(results[("manager1", 7000)], ManagerPingResponse)
        assert isinstance(results[("manager2", 7001)], ManagerPingResponse)

    @pytest.mark.asyncio
    async def test_ping_all_managers_partial_failure(self, discovery, send_tcp):
        """Test ping_all_managers when some fail."""
        async def mock_send(target, msg_type, data, timeout):
            if target[1] == 7000:
                response = ManagerPingResponse(
                request_id="req-1",
                manager_id="mgr-1",
                datacenter="dc-east",
                host="localhost",
                port=7000,
                is_leader=True,
                state="healthy",
                term=1,
                worker_count=3,
                active_job_count=5,
            )
                return (response.dump(), None)
            else:
                # Second manager fails
                return (ConnectionError("Timeout"), None)

        send_tcp.side_effect = mock_send

        results = await discovery.ping_all_managers()

        # One success, one failure
        assert len(results) == 2
        assert isinstance(results[("manager1", 7000)], ManagerPingResponse)
        assert isinstance(results[("manager2", 7001)], Exception)

    @pytest.mark.asyncio
    async def test_ping_all_gates_success(self, discovery, send_tcp):
        """Test pinging all gates concurrently."""
        async def mock_send(target, msg_type, data, timeout):
            if target[1] == 9000:
                response = GatePingResponse(
                request_id="req-1",
                gate_id="gate-1",
                datacenter="dc-1",
                host="localhost",
                port=9000,
                is_leader=True,
                state="healthy",
                term=1,
                active_datacenter_count=2,
                active_job_count=20,
            )
            else:
                response = GatePingResponse(
                request_id="req-2",
                gate_id="gate-2",
                datacenter="dc-1",
                host="localhost",
                port=9000,
                is_leader=True,
                state="healthy",
                term=1,
                active_datacenter_count=2,
                active_job_count=25,
            )
            return (response.dump(), None)

        send_tcp.side_effect = mock_send

        results = await discovery.ping_all_gates()

        assert len(results) == 2
        assert ("gate1", 9000) in results
        assert ("gate2", 9001) in results

    # =========================================================================
    # Workflow Query Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_happy_path_query_workflows(self, state, logger, send_tcp):
        """Test workflow query from managers."""
        # Use single-manager config to avoid duplicate results from parallel queries
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("manager1", 7000)],
            gates=[],
        )
        targets = ClientTargetSelector(config, state)
        discovery = ClientDiscovery(state, config, logger, targets, send_tcp)

        workflow_info = WorkflowStatusInfo(
            workflow_name="TestWorkflow",
            workflow_id="TestWorkflow-wf-1",
            job_id="job-123",
            status="running",
        )
        query_response = WorkflowQueryResponse(
            request_id="req-query-1",
            manager_id="mgr-1",
            datacenter="dc-east",
            workflows=[workflow_info],
        )
        send_tcp.return_value = (query_response.dump(), None)

        results = await discovery.query_workflows(["TestWorkflow"])

        assert "dc-east" in results
        assert len(results["dc-east"]) == 1
        assert results["dc-east"][0].workflow_name == "TestWorkflow"

    @pytest.mark.asyncio
    async def test_query_workflows_no_managers(self, state, logger, send_tcp):
        """Test query_workflows with no managers configured."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[],
        )
        targets = ClientTargetSelector(config, state)
        discovery = ClientDiscovery(state, config, logger, targets, send_tcp)

        with pytest.raises(RuntimeError, match="No managers configured"):
            await discovery.query_workflows(["TestWorkflow"])

    @pytest.mark.asyncio
    async def test_query_workflows_with_job_target(self, discovery, send_tcp, state):
        """Test workflow query when job target is known."""
        job_id = "job-target-123"
        # Mark job target in state
        state.mark_job_target(job_id, ("manager1", 7000))

        workflow_info = WorkflowStatusInfo(
            workflow_name="TestWorkflow",
            workflow_id="TestWorkflow-wf-1",
            job_id=job_id,
            status="completed",
        )
        query_response = WorkflowQueryResponse(
            request_id="req-query",
            manager_id="mgr-1",
            datacenter="dc-east",
            workflows=[workflow_info],
        )
        send_tcp.return_value = (query_response.dump(), None)

        results = await discovery.query_workflows(
            ["TestWorkflow"],
            job_id=job_id,
        )

        # Should query job target first and return those results
        assert "dc-east" in results
        send_tcp.assert_called_once()  # Only queries job target

    @pytest.mark.asyncio
    async def test_query_workflows_via_gate_success(self, discovery, send_tcp):
        """Test workflow query via gate."""
        workflow_info = WorkflowStatusInfo(
            workflow_name="GateWorkflow",
            workflow_id="GateWorkflow-wf-1",
            job_id="job-gate-1",
            status="running",
        )
        dc_status = DatacenterWorkflowStatus(
            dc_id="dc-east",
            workflows=[workflow_info],
        )
        gate_response = GateWorkflowQueryResponse(
            request_id="req-gate-query",
            gate_id="gate-1",
            datacenters=[dc_status],
        )
        send_tcp.return_value = (gate_response.dump(), None)

        results = await discovery.query_workflows_via_gate(["GateWorkflow"])

        assert "dc-east" in results
        assert len(results["dc-east"]) == 1
        assert results["dc-east"][0].workflow_name == "GateWorkflow"

    @pytest.mark.asyncio
    async def test_query_workflows_via_gate_no_gates(self, state, logger, send_tcp):
        """Test query via gate with no gates configured."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[],
        )
        targets = ClientTargetSelector(config, state)
        discovery = ClientDiscovery(state, config, logger, targets, send_tcp)

        with pytest.raises(RuntimeError, match="No gates configured"):
            await discovery.query_workflows_via_gate(["TestWorkflow"])

    @pytest.mark.asyncio
    async def test_query_workflows_via_gate_server_error(self, discovery, send_tcp):
        """Test query via gate when server returns error."""
        send_tcp.return_value = (b'error', None)

        with pytest.raises(RuntimeError, match="gate returned error"):
            await discovery.query_workflows_via_gate(["TestWorkflow"])

    @pytest.mark.asyncio
    async def test_query_all_gates_workflows_success(self, discovery, send_tcp):
        """Test querying workflows from all gates concurrently."""
        async def mock_send(target, msg_type, data, timeout):
            workflow_info = WorkflowStatusInfo(
            workflow_name="MultiGateWorkflow",
            workflow_id="MultiGateWorkflow-wf-1",
            job_id="job-multi",
            status="running",
        )
            dc_status = DatacenterWorkflowStatus(
                dc_id="dc-east",
                workflows=[workflow_info],
            )
            gate_response = GateWorkflowQueryResponse(
                request_id=secrets.token_hex(8),
                gate_id=f"gate-{target[1]}",
                datacenters=[dc_status],
            )
            return (gate_response.dump(), None)

        send_tcp.side_effect = mock_send

        results = await discovery.query_all_gates_workflows(["MultiGateWorkflow"])

        assert len(results) == 2
        assert ("gate1", 9000) in results
        assert ("gate2", 9001) in results
        # Both should return dict with datacenter results
        assert isinstance(results[("gate1", 9000)], dict)
        assert "dc-east" in results[("gate1", 9000)]

    # =========================================================================
    # Datacenter Discovery Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_happy_path_get_datacenters(self, discovery, send_tcp):
        """Test getting datacenter list from gate."""
        dc_info = DatacenterInfo(
            dc_id="dc-east",
            health="healthy",
            leader_addr=("manager1", 7000),
            available_cores=100,
            worker_count=10,
        )
        dc_response = DatacenterListResponse(
            request_id="req-dc",
            gate_id="gate-1",
            datacenters=[dc_info],
            total_available_cores=100,
            healthy_datacenter_count=1,
        )
        send_tcp.return_value = (dc_response.dump(), None)

        result = await discovery.get_datacenters(("gate1", 9000))

        assert result.gate_id == "gate-1"
        assert len(result.datacenters) == 1
        assert result.datacenters[0].dc_id == "dc-east"
        assert result.total_available_cores == 100

    @pytest.mark.asyncio
    async def test_get_datacenters_no_gates(self, state, logger, send_tcp):
        """Test get_datacenters with no gates configured."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[],
        )
        targets = ClientTargetSelector(config, state)
        discovery = ClientDiscovery(state, config, logger, targets, send_tcp)

        with pytest.raises(RuntimeError, match="No gates configured"):
            await discovery.get_datacenters()

    @pytest.mark.asyncio
    async def test_get_datacenters_server_error(self, discovery, send_tcp):
        """Test get_datacenters when server returns error."""
        send_tcp.return_value = (b'error', None)

        with pytest.raises(RuntimeError, match="gate returned error"):
            await discovery.get_datacenters(("gate1", 9000))

    @pytest.mark.asyncio
    async def test_get_datacenters_network_exception(self, discovery, send_tcp):
        """Test get_datacenters when network exception occurs."""
        send_tcp.return_value = (ConnectionError("Network down"), None)

        with pytest.raises(RuntimeError, match="Datacenter list query failed"):
            await discovery.get_datacenters(("gate1", 9000))

    @pytest.mark.asyncio
    async def test_get_datacenters_from_all_gates_success(self, discovery, send_tcp):
        """Test getting datacenters from all gates concurrently."""
        async def mock_send(target, msg_type, data, timeout):
            dc_info = DatacenterInfo(
            dc_id="dc-east",
            health="healthy",
            leader_addr=("manager1", 7000),
            available_cores=50,
            worker_count=5,
        )
            dc_response = DatacenterListResponse(
                request_id=secrets.token_hex(8),
                gate_id=f"gate-{target[1]}",
                datacenters=[dc_info],
                total_available_cores=50,
                healthy_datacenter_count=1,
            )
            return (dc_response.dump(), None)

        send_tcp.side_effect = mock_send

        results = await discovery.get_datacenters_from_all_gates()

        assert len(results) == 2
        assert ("gate1", 9000) in results
        assert ("gate2", 9001) in results
        assert isinstance(results[("gate1", 9000)], DatacenterListResponse)
        assert isinstance(results[("gate2", 9001)], DatacenterListResponse)

    @pytest.mark.asyncio
    async def test_get_datacenters_from_all_gates_partial_failure(self, discovery, send_tcp):
        """Test get_datacenters_from_all_gates with partial failures."""
        async def mock_send(target, msg_type, data, timeout):
            if target[1] == 9000:
                dc_info = DatacenterInfo(
            dc_id="dc-east",
            health="healthy",
            leader_addr=("manager1", 7000),
            available_cores=50,
            worker_count=5,
        )
                dc_response = DatacenterListResponse(
                    request_id=secrets.token_hex(8),
                    gate_id="gate-1",
                    datacenters=[dc_info],
                    total_available_cores=50,
                    healthy_datacenter_count=1,
                )
                return (dc_response.dump(), None)
            else:
                # Second gate fails
                return (ConnectionError("Timeout"), None)

        send_tcp.side_effect = mock_send

        results = await discovery.get_datacenters_from_all_gates()

        assert len(results) == 2
        assert isinstance(results[("gate1", 9000)], DatacenterListResponse)
        assert isinstance(results[("gate2", 9001)], Exception)

    # =========================================================================
    # Concurrency Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_concurrency_multiple_ping_operations(self, discovery, send_tcp):
        """Test concurrent ping operations to different targets."""
        # Mock different responses
        async def mock_send(target, msg_type, data, timeout):
            if target[1] >= 9000:  # Gate
                response = GatePingResponse(
                    request_id=secrets.token_hex(8),
                    gate_id=f"gate-{target[1]}",
                    datacenter="dc-1",
                    host="localhost",
                    port=target[1],
                    is_leader=True,
                    state="healthy",
                    term=1,
                    active_job_count=10,
                )
            else:  # Manager
                response = ManagerPingResponse(
                    request_id=secrets.token_hex(8),
                    manager_id=f"mgr-{target[1]}",
                    datacenter="dc-east",
                    host="localhost",
                    port=target[1],
                    is_leader=True,
                    state="healthy",
                    term=1,
                    worker_count=3,
                    active_job_count=5,
                )
            return (response.dump(), None)

        send_tcp.side_effect = mock_send

        # Ping both managers and gates concurrently
        manager_results, gate_results = await asyncio.gather(
            discovery.ping_all_managers(),
            discovery.ping_all_gates(),
        )

        assert len(manager_results) == 2
        assert len(gate_results) == 2

    @pytest.mark.asyncio
    async def test_concurrency_query_and_datacenter_operations(self, discovery, send_tcp):
        """Test concurrent query and datacenter discovery."""
        async def mock_send(target, msg_type, data, timeout):
            if msg_type == "workflow_query":
                workflow_info = WorkflowStatusInfo(
            workflow_name="TestWorkflow",
            workflow_id="TestWorkflow-wf-1",
            job_id="job-123",
            status="running",
        )
                dc_status = DatacenterWorkflowStatus(
                    dc_id="dc-east",
                    workflows=[workflow_info],
                )
                response = GateWorkflowQueryResponse(
                    request_id=secrets.token_hex(8),
                    gate_id="gate-1",
                    datacenters=[dc_status],
                )
            else:  # datacenter_list
                dc_info = DatacenterInfo(
            dc_id="dc-east",
            health="healthy",
            leader_addr=("manager1", 7000),
            available_cores=100,
            worker_count=10,
        )
                response = DatacenterListResponse(
                    request_id=secrets.token_hex(8),
                    gate_id="gate-1",
                    datacenters=[dc_info],
                    total_available_cores=100,
                    healthy_datacenter_count=1,
                )
            return (response.dump(), None)

        send_tcp.side_effect = mock_send

        # Run queries and datacenter discovery concurrently
        workflow_results, dc_results = await asyncio.gather(
            discovery.query_all_gates_workflows(["TestWorkflow"]),
            discovery.get_datacenters_from_all_gates(),
        )

        assert len(workflow_results) == 2
        assert len(dc_results) == 2

    # =========================================================================
    # Edge Case Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_edge_case_empty_workflow_list(self, discovery, send_tcp):
        """Test workflow query with empty workflow list."""
        query_response = WorkflowQueryResponse(
            request_id="req-empty",
            manager_id="mgr-1",
            datacenter="dc-east",
            workflows=[],  # Empty workflow list
        )
        send_tcp.return_value = (query_response.dump(), None)

        results = await discovery.query_workflows([])

        # Should still work with empty results
        assert isinstance(results, dict)

    @pytest.mark.asyncio
    async def test_edge_case_many_datacenters(self, discovery, send_tcp):
        """Test datacenter discovery with many datacenters."""
        datacenters = [
            DatacenterInfo(
                dc_id=f"dc-{i}",
                health="healthy",
                leader_addr=(f"manager{i}", 7000 + i),
                available_cores=100,
                worker_count=10,
            )
            for i in range(50)
        ]
        dc_response = DatacenterListResponse(
            request_id="req-many-dc",
            gate_id="gate-1",
            datacenters=datacenters,
            total_available_cores=5000,
            healthy_datacenter_count=50,
        )
        send_tcp.return_value = (dc_response.dump(), None)

        result = await discovery.get_datacenters(("gate1", 9000))

        assert len(result.datacenters) == 50
        assert result.total_available_cores == 5000

    @pytest.mark.asyncio
    async def test_edge_case_special_characters_in_ids(self, discovery, send_tcp):
        """Test discovery with special characters in IDs."""
        workflow_info = WorkflowStatusInfo(
            workflow_name="Test-Workflow_123-🚀",
            workflow_id="Test-Workflow_123-🚀-wf-1",
            job_id="job-ñ-中文",
            status="running",
        )
        query_response = WorkflowQueryResponse(
            request_id="req-special",
            manager_id="mgr-1",
            datacenter="dc-east-🌍",
            workflows=[workflow_info],
        )
        send_tcp.return_value = (query_response.dump(), None)

        results = await discovery.query_workflows(["Test-Workflow_123-🚀"])

        assert "dc-east-🌍" in results
        assert results["dc-east-🌍"][0].workflow_name == "Test-Workflow_123-🚀"

    @pytest.mark.asyncio
    async def test_edge_case_ping_with_custom_timeout(self, discovery, send_tcp):
        """Test ping operations with custom timeout values."""
        ping_response = ManagerPingResponse(
                request_id="req-timeout",
                manager_id="mgr-1",
                datacenter="dc-east",
                host="localhost",
                port=7000,
                is_leader=True,
                state="healthy",
                term=1,
                worker_count=5,
                active_job_count=10,
            )
        send_tcp.return_value = (ping_response.dump(), None)

        # Very short timeout
        await discovery.ping_manager(("manager1", 7000), timeout=0.1)

        # Very long timeout
        await discovery.ping_manager(("manager1", 7000), timeout=60.0)

        # Should work with both
        assert send_tcp.call_count == 2
