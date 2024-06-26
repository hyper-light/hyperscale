from __future__ import annotations

import uuid
from typing import Any, Dict, List, Tuple, Union

from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.types.stage_types import StageTypes
from hyperscale.core.hooks.types.base.simple_context import SimpleContext

from .act.act_edge import ActEdge
from .analyze.analyze_edge import AnalyzeEdge, BaseEdge
from .common.complete_edge import CompleteEdge
from .common.error_edge import ErrorEdge
from .common.transtition_metadata import TransitionMetadata
from .execute.execute_edge import ExecuteEdge
from .idle.idle_edge import IdleEdge
from .optimize.optimize_edge import OptimizeEdge
from .setup.setup_edge import SetupEdge
from .submit.submit_edge import SubmitEdge

HistoryUpdate = Dict[Tuple[str, str], Any]


class Transition:
    def __init__(
        self, metadata: TransitionMetadata, from_stage: Stage, to_stage: Stage
    ) -> None:
        self.transition_id = str(uuid.uuid4())
        self.metadata = metadata
        self.from_stage = from_stage
        self.to_stage = to_stage
        self.edges: List[BaseEdge] = []
        self.edges_by_name: Dict[Tuple[str, str], BaseEdge] = {}
        self.adjacency_list: Dict[str, List[Transition]] = []
        self.predecessors = []
        self.descendants = []
        self.destinations: List[str] = []
        self.transition_idx = 0
        self.retries = from_stage.stage_retries
        edge_types = {
            StageTypes.ACT: ActEdge,
            StageTypes.ANALYZE: AnalyzeEdge,
            StageTypes.COMPLETE: CompleteEdge,
            StageTypes.ERROR: ErrorEdge,
            StageTypes.EXECUTE: ExecuteEdge,
            StageTypes.IDLE: IdleEdge,
            StageTypes.OPTIMIZE: OptimizeEdge,
            StageTypes.SETUP: SetupEdge,
            StageTypes.SUBMIT: SubmitEdge,
        }

        self.edge: BaseEdge = edge_types.get(from_stage.stage_type)(
            from_stage, to_stage
        )

    async def execute(self):
        self.edge.setup()

        if self.edge.source.context is None:
            self.edge.source.context = SimpleContext()

        result: Union[None, Tuple[None, StageTypes]] = None
        if self.retries > 0:
            for _ in range(self.retries):
                result = await self.edge.transition()

                if self.edge.exception is None:
                    break

        else:
            result = await self.edge.transition()

        self.edge.descendants = {
            descendant: self.edges_by_name.get((self.edge.source.name, descendant))
            for descendant in self.descendants
        }

        skip_next_stages = [
            StageTypes.COMPLETE,
            StageTypes.ERROR,
        ]

        is_ignored_stage = self.to_stage.stage_type in skip_next_stages
        stage_skipped = self.edge.source.skip is True and self.edge.skip_stage is False
        invalid_transition = self.metadata.is_valid is False
        has_exception = self.edge.exception is not None

        pass_to_next = (
            is_ignored_stage or stage_skipped or invalid_transition or has_exception
        ) is False

        if pass_to_next:
            source_name = self.edge.source.name
            selected_edge = self.edge

            if self.edge.skip_stage:
                selected_edge_idx = min([edge.transition_idx for edge in self.edges])

                selected_edge = self.edges[selected_edge_idx]
                source_name = self.edges[selected_edge_idx].source.name

            destination_name = self.edge.destination.name

            neighbors: List[Tuple[str, str]] = []
            transition_source_histories: Dict[str, HistoryUpdate] = {}

            source_edge_name = (source_name, destination_name)

            transition_source_history: HistoryUpdate = selected_edge.next_history.get(
                source_edge_name, {}
            )

            for destination in self.destinations:
                for transition in self.adjacency_list[destination]:
                    destination_edge_name = (
                        destination,
                        transition.edge.destination.name,
                    )

                    neighbors.extend(
                        [
                            (destination, transition.edge.destination.name)
                            for transition in self.adjacency_list[destination]
                        ]
                    )

                    transition_source_histories[destination_edge_name] = (
                        transition_source_history
                    )

            for neighbor in neighbors:
                required_keys = self.edges_by_name[neighbor].requires

                if source_name not in self.edges_by_name[neighbor].from_stage_names:
                    self.edges_by_name[neighbor].from_stage_names.append(source_name)

                for edge_name in self.edge.next_history:
                    source_history: HistoryUpdate = self.edge.next_history[edge_name]

                    self.edges_by_name[neighbor].history.update(
                        {
                            edge_name: {
                                key: value
                                for key, value in source_history.items()
                                if key in required_keys
                            }
                        }
                    )

                neighbor_edge_source = self.edges_by_name[neighbor].source.name

                previous_edge = (source_name, neighbor_edge_source)

                for history in transition_source_histories.values():
                    if (
                        self.edges_by_name[neighbor].history.get(previous_edge) is None
                        and len(history) > 0
                    ):
                        self.edges_by_name[neighbor].history.update(
                            {
                                previous_edge: {
                                    key: value
                                    for key, value in history.items()
                                    if key in required_keys
                                }
                            }
                        )

        self.edge.edge_data = {}

        return result
