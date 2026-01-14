from dataclasses import dataclass, field
import time

from hyperscale.graph import Workflow

from tests.framework.runtime.callback_tracker import CallbackTracker
from tests.framework.runtime.cluster_factory import ClusterFactory
from tests.framework.runtime.test_cluster import TestCluster
from tests.framework.specs.scenario_spec import ScenarioSpec


@dataclass(slots=True)
class ScenarioRuntime:
    spec: ScenarioSpec
    workflow_registry: dict[str, type[Workflow]]
    cluster_factory: ClusterFactory = field(default_factory=ClusterFactory)
    cluster: TestCluster | None = None
    callbacks: CallbackTracker = field(default_factory=CallbackTracker)
    job_ids: dict[str, str] = field(default_factory=dict)
    last_job_id: str | None = None
    started_at: float = field(default_factory=time.monotonic)

    async def start_cluster(self) -> None:
        if self.cluster:
            raise RuntimeError("Cluster already started")
        self.cluster = await self.cluster_factory.create_cluster(self.spec.cluster)

    async def stop_cluster(self) -> None:
        if not self.cluster:
            return
        await self.cluster_factory.teardown_cluster(self.cluster)
        self.cluster = None

    def require_cluster(self) -> TestCluster:
        if not self.cluster:
            raise RuntimeError("Cluster not started")
        return self.cluster

    def resolve_workflow(self, name: str) -> type[Workflow]:
        if name not in self.workflow_registry:
            raise ValueError(f"Unknown workflow '{name}'")
        return self.workflow_registry[name]
