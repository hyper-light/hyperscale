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

    def availalble(self):
        return self.sem._value

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
                    "vus",
                ],
                str | int | StagePriority,
            ]
        ],
    ) -> List[List[Tuple[str, StagePriority, int]]]:
        """
        Allocate cores to workflows based on priority and VUs.

        Allocation strategy (matches WorkflowDispatcher._calculate_allocations):
        1. Non-test workflows get 0 cores (bypass partition)
        2. EXCLUSIVE workflows get ALL cores, blocking others
        3. Explicit priority workflows (HIGH/NORMAL/LOW) allocated proportionally by VUs
        4. AUTO priority workflows split remaining cores equally (minimum 1 each)

        Returns list containing a single batch with all allocations.
        """
        if not configs:
            return []

        total_cores = self.max_workers
        allocations: List[Tuple[str, StagePriority, int]] = []

        # Separate non-test workflows (they bypass partitioning with 0 cores)
        non_test_workflows: List[Tuple[str, StagePriority, int]] = []
        test_workflows: List[Dict[str, Any]] = []

        for config in configs:
            workflow_name = config.get("workflow_name")
            priority = config.get("priority", StagePriority.AUTO)

            if not config.get("is_test", False):
                non_test_workflows.append((workflow_name, priority, 0))
            else:
                test_workflows.append(config)

        # Add non-test workflows to allocations (0 cores each)
        allocations.extend(non_test_workflows)

        if not test_workflows:
            return [allocations] if allocations else []

        # Check for EXCLUSIVE workflows first - they get all cores
        exclusive_workflows = [
            config for config in test_workflows
            if config.get("priority", StagePriority.AUTO) == StagePriority.EXCLUSIVE
        ]

        if exclusive_workflows:
            # First EXCLUSIVE workflow gets all cores, others get 0
            first_exclusive = exclusive_workflows[0]
            allocations.append((
                first_exclusive.get("workflow_name"),
                StagePriority.EXCLUSIVE,
                total_cores,
            ))

            # Remaining exclusive workflows get 0 (will wait)
            for config in exclusive_workflows[1:]:
                allocations.append((
                    config.get("workflow_name"),
                    StagePriority.EXCLUSIVE,
                    0,
                ))

            # Non-exclusive test workflows also get 0 while exclusive runs
            for config in test_workflows:
                if config not in exclusive_workflows:
                    allocations.append((
                        config.get("workflow_name"),
                        config.get("priority", StagePriority.AUTO),
                        0,
                    ))

            return [allocations]

        # Separate explicit priority from AUTO workflows
        explicit_priority_workflows = [
            config for config in test_workflows
            if config.get("priority", StagePriority.AUTO) != StagePriority.AUTO
        ]
        auto_workflows = [
            config for config in test_workflows
            if config.get("priority", StagePriority.AUTO) == StagePriority.AUTO
        ]

        remaining_cores = total_cores

        # Step 1: Allocate explicit priority workflows (proportionally by VUs)
        if explicit_priority_workflows:
            # Sort by priority (higher value = higher priority) then by VUs (higher first)
            explicit_priority_workflows = sorted(
                explicit_priority_workflows,
                key=lambda config: (
                    -config.get("priority", StagePriority.AUTO).value,
                    -config.get("vus", 1000),
                ),
            )

            # Calculate total VUs for proportional allocation
            total_vus = sum(config.get("vus", 1000) for config in explicit_priority_workflows)
            if total_vus == 0:
                total_vus = len(explicit_priority_workflows)

            for index, config in enumerate(explicit_priority_workflows):
                if remaining_cores <= 0:
                    # No more cores - remaining workflows get 0
                    allocations.append((
                        config.get("workflow_name"),
                        config.get("priority", StagePriority.AUTO),
                        0,
                    ))
                    continue

                workflow_vus = config.get("vus", 1000)

                # Last explicit workflow gets remaining if no AUTO workflows
                if index == len(explicit_priority_workflows) - 1 and not auto_workflows:
                    cores = remaining_cores
                else:
                    # Proportional allocation by VUs
                    share = workflow_vus / total_vus if total_vus > 0 else 1 / len(explicit_priority_workflows)
                    cores = max(1, int(total_cores * share))
                    cores = min(cores, remaining_cores)

                allocations.append((
                    config.get("workflow_name"),
                    config.get("priority", StagePriority.AUTO),
                    cores,
                ))
                remaining_cores -= cores

        # Step 2: Split remaining cores equally among AUTO workflows (min 1 each)
        if auto_workflows and remaining_cores > 0:
            # Only allocate as many workflows as we have cores for
            num_auto_to_allocate = min(len(auto_workflows), remaining_cores)
            cores_per_auto = remaining_cores // num_auto_to_allocate
            leftover = remaining_cores - (cores_per_auto * num_auto_to_allocate)

            for index, config in enumerate(auto_workflows):
                if index >= num_auto_to_allocate:
                    # No more cores - remaining AUTO workflows get 0
                    allocations.append((
                        config.get("workflow_name"),
                        StagePriority.AUTO,
                        0,
                    ))
                    continue

                # Give one extra core to first workflows if there's leftover
                cores = cores_per_auto + (1 if index < leftover else 0)

                allocations.append((
                    config.get("workflow_name"),
                    StagePriority.AUTO,
                    cores,
                ))
                remaining_cores -= cores

        elif auto_workflows:
            # No remaining cores - all AUTO workflows get 0
            for config in auto_workflows:
                allocations.append((
                    config.get("workflow_name"),
                    StagePriority.AUTO,
                    0,
                ))

        return [allocations]
