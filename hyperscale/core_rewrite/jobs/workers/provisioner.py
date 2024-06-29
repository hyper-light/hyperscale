import asyncio
import math
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
)

import psutil

from .batched_semaphore import BatchedSemaphore
from .stage_priority import StagePriority


class Provisioner:
    def __init__(self) -> None:
        cpu_cores = psutil.cpu_count(logical=False)
        self._cpu_cores = cpu_cores
        self.max_workers: int = None

        self.loop: Optional[asyncio.AbstractEventLoop] = None

        self.batch_by_stages = False

    def setup(self, max_workers: int | None = None):
        if max_workers is None:
            max_workers = self._cpu_cores

        self.max_workers = min(max_workers, self._cpu_cores)

        self.loop = asyncio.get_event_loop()
        self.sem = BatchedSemaphore(self.max_workers)

    async def acquire(self, count: int):
        await self.sem.acquire(count)

    def release(self, count: int):
        self.sem.release(count)

    def partition(
        self,
        stages: List[Any],
    ) -> List[Tuple[str, Any, int]]:
        # How many batches do we have? For example -> 5 stages over 4
        # CPUs means 2 batches. The first batch will assign one stage to
        # each core. The second will assign all four cores to the remaing
        # one stage.

        batches = []

        if self.batch_by_stages is False:
            stages_count = len(stages)
            self.batch_sets_count = math.ceil(stages_count / self.max_workers)
            if self.batch_sets_count < 1:
                self.batch_sets_count = 1

            self.more_stages_than_cpus = stages_count / self.max_workers > 1

            if stages_count % self.max_workers > 0 and stages_count > self.max_workers:
                batch_size = self.max_workers
                workers_per_stage = int(self.max_workers / batch_size)
                batched_stages = [
                    [workers_per_stage for _ in range(batch_size)]
                    for _ in range(self.batch_sets_count - 1)
                ]

            else:
                batch_size = int(stages_count / self.batch_sets_count)
                workers_per_stage = int(self.max_workers / batch_size)
                batched_stages = [
                    [workers_per_stage for _ in range(batch_size)]
                    for _ in range(self.batch_sets_count)
                ]

            batches_count = len(batched_stages)

            # If we have a remainder batch - i.e. more stages than cores.
            last_batch = batched_stages[batches_count - 1]

            last_batch_size = stages_count % self.max_workers
            if last_batch_size > 0 and self.more_stages_than_cpus:
                last_batch_workers = int(self.max_workers / last_batch_size)
                batched_stages.append(
                    [last_batch_workers for _ in range(last_batch_size)]
                )

            last_batch = batched_stages[self.batch_sets_count - 1]
            last_batch_size = len(last_batch)
            last_batch_remainder = self.max_workers % last_batch_size

            if last_batch_remainder > 0:
                for idx in range(last_batch_remainder):
                    last_batch[idx] += 1

            stage_idx = 0

            for batch in batched_stages:
                for stage_idx, stage_workers_count in enumerate(batch):
                    stage_name, stage = stages[stage_idx]

                    batches.append((stage_name, stage, stage_workers_count))

        else:
            for stage_name, stage in stages:
                batches.append((stage_name, stage, 1))

        return batches

    def partion_by_priority(
        self,
        configs: List[
            Dict[
                Literal[
                    "workflow_name",
                    "priority",
                    "is_test",
                    "threads",
                ],
                str | int | StagePriority,
            ]
        ],
    ) -> List[List[Tuple[str, StagePriority, int]]]:
        # How many batches do we have? For example -> 5 stages over 4
        # CPUs means 2 batches. The first batch will assign one stage to
        # each core. The second will assign all four cores to the remaing
        # one stage.

        batches: List[List[Tuple[str, StagePriority, int]]] = []
        seen: List[Any] = []

        sorted_priority_configs = list(
            sorted(
                configs,
                key=lambda config: config.get(
                    "priority",
                    StagePriority.AUTO,
                ).value
                if config.get("is_test", False)
                else 0,
                reverse=True,
            )
        )

        bypass_partition_batch: List[Tuple[str, StagePriority, int]] = []
        for config in sorted_priority_configs:
            if config.get("is_test", False) is False:
                bypass_partition_batch.append(
                    (
                        config.get("workflow_name"),
                        config.get(
                            "priority",
                            StagePriority.AUTO,
                        ),
                        0,
                    )
                )

                seen.append(config.get("workflow_name"))

        if len(bypass_partition_batch) > 0:
            batches.append(bypass_partition_batch)

        workflow_configs: Dict[
            str,
            Dict[str, int],
        ] = {config.get("workflow_name"): config for config in sorted_priority_configs}

        parallel_workflows_count = len(
            [config for config in workflow_configs.values() if config.get("is_test")]
        )

        stages_count = len(workflow_configs)

        auto_workflows_count = len(
            [
                config
                for config in workflow_configs.values()
                if config.get("priority", StagePriority.AUTO) == StagePriority.AUTO
            ]
        )

        min_workers_counts: Dict[str, int] = {}
        max_workers_counts: Dict[str, int] = {}

        for config in sorted_priority_configs:
            if config.get("is_test", False):
                worker_allocation_range: Tuple[int, int] = (
                    StagePriority.get_worker_allocation_range(
                        config.get(
                            "priority",
                            StagePriority.AUTO,
                        ),
                        self.max_workers,
                    )
                )

                minimum_workers, maximum_workers = worker_allocation_range

                workflow_name = config.get("workflow_name")
                min_workers_counts[workflow_name] = minimum_workers
                max_workers_counts[workflow_name] = maximum_workers

        if parallel_workflows_count == 1:
            parallel_workflows = [
                config
                for config in sorted_priority_configs
                if config.get("is_test", False)
            ]

            workflow = parallel_workflows.pop()

            workflow_group = [
                (
                    workflow.get("workflow_name"),
                    workflow.get("priority", StagePriority.AUTO),
                    workflow.get("threads", self.max_workers),
                )
            ]

            return [workflow_group]

        elif auto_workflows_count == stages_count and parallel_workflows_count > 0:
            # All workflows are auto priority so evently bin the threads between
            # workflows.
            parallel_auto_workflows = len(
                [
                    config
                    for config in workflow_configs.values()
                    if config.get(
                        "priority",
                        StagePriority.AUTO,
                    )
                    == StagePriority.AUTO
                    and config.get(
                        "is_test",
                        False,
                    )
                ]
            )
            threads_count = max(
                math.floor(self.max_workers / parallel_auto_workflows), 1
            )

            remainder = self.max_workers % parallel_auto_workflows

            threads_counts = [threads_count for _ in range(parallel_auto_workflows)]

            for idx in range(remainder):
                threads_counts[idx] += 1

            workflows_group = [
                (
                    config.get("workflow_name"),
                    config.get("priority", StagePriority.AUTO),
                    threads,
                )
                for threads, config in zip(
                    threads_counts,
                    sorted_priority_configs,
                )
            ]

            return [workflows_group]

        else:
            for config in sorted_priority_configs:
                if config.get("workflow_name") not in seen:
                    # So for example 8 - 4 = 4 we need another stage with 4
                    batch_workers_allocated: int = max_workers_counts.get(
                        config.get("workflow_name"),
                        0,
                    )

                    workflow_group: List[
                        Tuple[
                            str,
                            StagePriority,
                            int,
                        ]
                    ] = [
                        (
                            config.get("workflow_name"),
                            config.get("priority", StagePriority.AUTO),
                            batch_workers_allocated,
                        )
                    ]

                    for other_config in sorted_priority_configs:
                        if (
                            other_config != config
                            and other_config.get("workflow_name") not in seen
                        ):
                            workflow_name = config.get("workflow_name")
                            workers_allocated: int = max_workers_counts.get(
                                workflow_name, 0
                            )

                            other_workflow_name = other_config.get("workflow_name")
                            min_workers = min_workers_counts.get(other_workflow_name)

                            current_allocation = (
                                batch_workers_allocated + workers_allocated
                            )

                            while (
                                current_allocation > self.max_workers
                                and workers_allocated >= min_workers
                            ):
                                workers_allocated -= 1
                                current_allocation = (
                                    batch_workers_allocated + workers_allocated
                                )

                            if (
                                current_allocation <= self.max_workers
                                and workers_allocated > 0
                            ):
                                batch_workers_allocated += workers_allocated
                                workflow_group.append(
                                    (
                                        other_config.get("workflow_name"),
                                        other_config.get(
                                            "priority", StagePriority.AUTO
                                        ),
                                        workers_allocated,
                                    )
                                )

                                seen.append(other_config.get("workflow_name"))

                    batches.append(workflow_group)
                    seen.append(config.get("workflow_name"))

            if parallel_workflows_count <= self.max_workers:
                for workflow_group in batches:
                    total_workers = sum([workers for _, _, workers in workflow_group])
                    group_size = len(workflow_group)

                    completed: List[str] = []

                    while (
                        total_workers < self.max_workers and len(completed) < group_size
                    ):
                        priority_sorted = list(
                            sorted(
                                workflow_group,
                                key=lambda workers_config: workers_config[1].value,
                                reverse=True,
                            )
                        )

                        remaining = sum([count for _, _, count in priority_sorted])

                        for idx, group in enumerate(priority_sorted):
                            name, priority, count = group

                            worker_max = max_workers_counts.get(name, 0)

                            max_increase = worker_max - remaining

                            if max_increase > 0:
                                while max_increase > 0:
                                    count += 1
                                    total_workers += 1
                                    max_increase -= 1

                                completed.append(name)

                            elif count < worker_max:
                                count += 1
                                total_workers += 1

                            else:
                                completed.append(name)

                            workflow_group[idx] = (
                                name,
                                priority,
                                count,
                            )

        return batches
