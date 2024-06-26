from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from typing import Any, Dict, List, Union

from hyperscale.core.engines.client.config import Config
from hyperscale.core.graphs.stages.act.act import Act
from hyperscale.core.graphs.stages.analyze.analyze import Analyze
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.types.stage_states import StageStates
from hyperscale.core.graphs.stages.types.stage_types import StageTypes
from hyperscale.core.graphs.transitions.common.base_edge import BaseEdge
from hyperscale.core.hooks.types.action.hook import ActionHook
from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.base.registrar import registrar
from hyperscale.core.hooks.types.base.simple_context import SimpleContext
from hyperscale.core.hooks.types.task.hook import TaskHook
from hyperscale.core.personas.streaming.stream_analytics import StreamAnalytics
from hyperscale.reporting.system.system_metrics_set_types import MonitorGroup

ExecuteHooks = List[Union[ActionHook, TaskHook]]


class ActEdge(BaseEdge[Act]):
    def __init__(self, source: Act, destination: BaseEdge[Stage]) -> None:
        super(ActEdge, self).__init__(source, destination)

        self.requires = [
            "analyze_stage_summary_metrics",
            "execute_stage_setup_config",
            "execute_stage_setup_by",
            "execute_stage_setup_hooks",
            "execute_stage_results",
            "execute_stage_streamed_analytics",
            "setup_stage_configs",
            "setup_stage_experiment_config",
            "setup_stage_ready_stages",
            "setup_stage_candidates",
            "session_stage_monitors",
        ]

        self.provides = [
            "analyze_stage_summary_metrics",
            "execute_stage_setup_hooks",
            "execute_stage_setup_config",
            "execute_stage_setup_by",
            "execute_stage_results",
            "execute_stage_streamed_analytics",
            "setup_stage_configs",
            "setup_stage_ready_stages",
            "setup_stage_experiment_config",
            "act_stage_monitors",
            "session_stage_monitors",
        ]

        self.valid_states = [StageStates.SETUP, StageStates.OPTIMIZED]

        self.assigned_candidates = []

    async def transition(self):
        try:
            self.source.state = StageStates.ACTING

            act_stages = self.stages_by_type.get(StageTypes.ACT)
            analyze_stages: Dict[str, Stage] = self.generate_analyze_candidates()

            self.source.context.update(self.edge_data)

            for event in self.source.dispatcher.events_by_name.values():
                event.source.stage_instance = self.source
                event.context.update(self.edge_data)

                if event.source.context:
                    event.source.context.update(self.edge_data)

            if self.timeout and self.skip_stage is False:
                await asyncio.wait_for(self.source.run(), timeout=self.timeout)

            elif self.skip_stage is False:
                await self.source.run()

            for provided in self.provides:
                self.edge_data[provided] = self.source.context[provided]

            if self.destination.context is None:
                self.destination.context = SimpleContext()

            self._update(self.destination)

            all_paths = self.all_paths.get(self.source.name, [])

            for stage in analyze_stages.values():
                if stage.name in all_paths:
                    if stage.context is None:
                        stage.context = SimpleContext()

                    self._update(stage)

            if self.destination.stage_type == StageTypes.SETUP:
                act_stages = list(self.stages_by_type.get(StageTypes.ACT).values())

                for stage in act_stages:
                    if (
                        stage.name not in self.visited
                        and stage.state == StageStates.SETUP
                    ):
                        stage.state = StageStates.INITIALIZED

            self.source.state = StageStates.ACTED

            self.visited.append(self.source.name)

        except Exception as edge_exception:
            self.exception = edge_exception

        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        for edge_name in self.history:
            history = self.history[edge_name]

            if self.next_history.get(edge_name) is None:
                self.next_history[edge_name] = {}

            self.next_history[edge_name].update(
                {key: value for key, value in history.items() if key in self.provides}
            )

        next_results = self.next_history.get((self.source.name, destination.name))
        if next_results is None:
            next_results = {}

        if self.skip_stage is False:
            session_stage_monitors: MonitorGroup = self.edge_data[
                "session_stage_monitors"
            ]
            session_stage_monitors.update(self.edge_data["act_stage_monitors"])

            next_results.update(
                {**self.edge_data, "session_stage_monitors": session_stage_monitors}
            )

            self.next_history.update(
                {(self.source.name, destination.name): next_results}
            )

    def split(self, edges: List[ActEdge]) -> None:
        analyze_stage_config: Dict[str, Any] = self.source.to_copy_dict()

        act_stage_copy: Act = type(self.source.name, (Act,), self.source.__dict__)()

        for (
            copied_attribute_name,
            copied_attribute_value,
        ) in analyze_stage_config.items():
            if inspect.ismethod(copied_attribute_value) is False:
                setattr(act_stage_copy, copied_attribute_name, copied_attribute_value)

        user_hooks: Dict[str, Dict[str, Hook]] = defaultdict(dict)
        for hooks in registrar.all.values():
            for hook in hooks:
                if hasattr(self.source, hook.shortname) and not hasattr(
                    Act, hook.shortname
                ):
                    user_hooks[self.source.name][hook.shortname] = hook._call

        act_stage_copy.dispatcher = self.source.dispatcher.copy()

        for event in act_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = act_stage_copy

        minimum_edge_idx = min([edge.transition_idx for edge in edges])

        act_stage_copy.context = SimpleContext()
        for event in act_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = act_stage_copy
            event.source.stage_instance.context = act_stage_copy.context
            event.source.context = act_stage_copy.context

            if event.source.shortname in user_hooks[act_stage_copy.name]:
                hook_call = user_hooks[act_stage_copy.name].get(event.source.shortname)

                hook_call = hook_call.__get__(act_stage_copy, act_stage_copy.__class__)
                setattr(act_stage_copy, event.source.shortname, hook_call)

                event.source._call = hook_call

            else:
                event.source._call = getattr(act_stage_copy, event.source.shortname)
                event.source._call = event.source._call.__get__(
                    act_stage_copy, act_stage_copy.__class__
                )
                setattr(act_stage_copy, event.source.shortname, event.source._call)

        self.source = act_stage_copy

        if minimum_edge_idx < self.transition_idx:
            self.skip_stage = True

    def _generate_edge_analyze_candidates(self, edges: List[ActEdge]):
        candidates = []

        for edge in edges:
            if edge.transition_idx != self.transition_idx:
                analyze_candidates = edge.generate_analyze_candidates()
                destination_path = edge.all_paths.get(edge.destination.name)
                candidates.extend(
                    [
                        candidate_name
                        for candidate_name in analyze_candidates
                        if candidate_name in destination_path
                    ]
                )

        return candidates

    def generate_analyze_candidates(self) -> Dict[str, Stage]:
        submit_stages: Dict[str, Analyze] = self.stages_by_type.get(StageTypes.ANALYZE)
        analyze_stages = self.stages_by_type.get(StageTypes.ACT).items()
        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        all_paths = self.all_paths.get(self.source.name, [])

        analyze_stages_in_path = {}
        for stage_name, stage in analyze_stages:
            if (
                stage_name in all_paths
                and stage_name != self.source.name
                and stage_name not in self.visited
            ):
                analyze_stages_in_path[stage_name] = self.all_paths.get(stage_name)

        submit_candidates: Dict[str, Stage] = {}

        for stage_name, stage in submit_stages.items():
            if stage_name in all_paths:
                if len(analyze_stages_in_path) > 0:
                    for path in analyze_stages_in_path.values():
                        if stage_name not in path:
                            submit_candidates[stage_name] = stage

                else:
                    submit_candidates[stage_name] = stage

        selected_submit_candidates: Dict[str, Stage] = {}
        following_opimize_stage_distances = [
            path_length
            for stage_name, path_length in path_lengths.items()
            if stage_name in analyze_stages
        ]

        for stage_name in path_lengths.keys():
            stage_distance = path_lengths.get(stage_name)

            if stage_name in submit_candidates:
                if len(following_opimize_stage_distances) > 0 and stage_distance < min(
                    following_opimize_stage_distances
                ):
                    selected_submit_candidates[stage_name] = submit_candidates.get(
                        stage_name
                    )

                elif len(following_opimize_stage_distances) == 0:
                    selected_submit_candidates[stage_name] = submit_candidates.get(
                        stage_name
                    )

        return selected_submit_candidates

    def setup(self) -> None:
        max_batch_size = 0
        execute_stage_setup_config: Config = None
        execute_stage_setup_hooks: Dict[str, ExecuteHooks] = {}
        execute_stage_setup_by: str = None
        execute_stage_streamed_analytics: Dict[str, List[StreamAnalytics]] = (
            defaultdict(list)
        )
        setup_stage_ready_stages: List[Stage] = []
        setup_stage_candidates: List[Stage] = []
        setup_stage_configs: Dict[str, Config] = {}
        setup_stage_experiment_config: Dict[str, Union[str, int, List[float]]] = {}

        for source_stage, destination_stage in self.history:
            previous_history: Dict[str, Any] = self.history[
                (source_stage, destination_stage)
            ]

            if destination_stage == self.source.name:
                execute_config: Config = previous_history.get(
                    "execute_stage_setup_config", Config()
                )
                setup_stage_configs.update(
                    previous_history.get("setup_stage_configs", {})
                )

                setup_by = previous_history.get("execute_stage_setup_by")

                if execute_config.optimized:
                    execute_stage_setup_config = execute_config
                    max_batch_size = execute_config.batch_size
                    execute_stage_setup_by = setup_by

                elif execute_config.batch_size > max_batch_size:
                    execute_stage_setup_config = execute_config
                    max_batch_size = execute_config.batch_size
                    execute_stage_setup_by = setup_by

                execute_hooks: ExecuteHooks = previous_history.get(
                    "execute_stage_setup_hooks", {}
                )
                for setup_hook in execute_hooks:
                    execute_stage_setup_hooks[setup_hook.name] = setup_hook

                ready_stages = previous_history.get("setup_stage_ready_stages", [])

                for ready_stage in ready_stages:
                    if ready_stage not in setup_stage_ready_stages:
                        setup_stage_ready_stages.append(ready_stage)

                stage_candidates: List[Stage] = previous_history.get(
                    "setup_stage_candidates", []
                )
                for stage_candidate in stage_candidates:
                    if stage_candidate not in setup_stage_candidates:
                        setup_stage_candidates.append(stage_candidate)

                stage_distributions = previous_history.get(
                    "setup_stage_experiment_config"
                )

                if stage_distributions:
                    setup_stage_experiment_config.update(stage_distributions)

            streamed_analytics = previous_history.get(
                "execute_stage_streamed_analytics"
            )

            if streamed_analytics:
                execute_stage_streamed_analytics[source_stage].extend(
                    streamed_analytics
                )

        raw_results = {}
        for from_stage_name in self.from_stage_names:
            stage_results = self.history[(from_stage_name, self.source.name)].get(
                "execute_stage_results"
            )

            if stage_results:
                raw_results.update(stage_results)

        act_stages = self.stages_by_type.get(StageTypes.ACT)

        results_to_calculate = {}
        target_stages = {}
        for stage_name in raw_results.keys():
            stage = act_stages.get(stage_name)
            all_paths = self.all_paths.get(stage_name)

            in_path = self.source.name in all_paths

            if in_path:
                stage.state = StageStates.ANALYZING
                results_to_calculate[stage_name] = raw_results.get(stage_name)
                target_stages[stage_name] = stage

        self.edge_data = {
            "setup_stage_configs": setup_stage_configs,
            "analyze_stage_raw_results": results_to_calculate,
            "analyze_stage_target_stages": target_stages,
            "analyze_stage_has_results": len(results_to_calculate) > 0,
            "execute_stage_setup_config": execute_stage_setup_config,
            "execute_stage_setup_hooks": execute_stage_setup_hooks,
            "execute_stage_setup_by": execute_stage_setup_by,
            "setup_stage_ready_stages": setup_stage_ready_stages,
            "setup_stage_candidates": setup_stage_candidates,
        }
