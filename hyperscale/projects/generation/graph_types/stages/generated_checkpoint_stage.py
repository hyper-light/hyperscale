import os
from typing import Any, Dict, List

from hyperscale.core.engines.types.common.base_result import BaseResult
from hyperscale.core.graphs.stages import Checkpoint
from hyperscale.core.hooks.types import save


class CheckpointStage(Checkpoint):
    @save(save_path=f"{os.getcwd()}/checkpoint.json")
    async def save_results(
        self, results: List[BaseResult] = []
    ) -> List[Dict[str, Any]]:
        return [
            {"action_id": data.action_id, "elapsed": data.complete - data.start}
            for data in results
        ]
