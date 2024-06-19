from typing import Awaitable

from hyperscale.core.engines.types.common.base_result import BaseResult
from hyperscale.core.graphs.stages import Validate
from hyperscale.core.hooks import validate


class ValidateStage(Validate):

    @validate('<action_name_here>')
    async def validate_action(self, action_or_task: Awaitable[BaseResult]):
        result: BaseResult = await action_or_task()
        assert result is not None
        assert result.action_id is not None
        assert result.error is False