import asyncio
import time

from tests.framework.actions.default_registry import build_default_registry
from tests.framework.results.scenario_outcome import ScenarioOutcome
from tests.framework.results.scenario_result import ScenarioResult
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.scenario_spec import ScenarioSpec


class ScenarioRunner:
    def __init__(self, workflow_registry: dict) -> None:
        self._workflow_registry = workflow_registry
        self._registry = build_default_registry()

    async def run(self, spec: ScenarioSpec) -> ScenarioOutcome:
        runtime = ScenarioRuntime(spec=spec, workflow_registry=self._workflow_registry)
        start = time.monotonic()
        outcome = ScenarioOutcome(
            name=spec.name,
            result=ScenarioResult.PASSED,
            duration_seconds=0.0,
        )
        try:
            for index, action in enumerate(spec.actions, start=1):
                handler = self._registry.get(action.action_type)
                action_timeout = action.timeout_seconds
                if action_timeout is None:
                    action_timeout = spec.timeouts.get(action.action_type)
                action_started = time.monotonic()
                try:
                    if action_timeout:
                        result = await asyncio.wait_for(
                            handler(runtime, action), timeout=action_timeout
                        )
                    else:
                        result = await handler(runtime, action)
                except asyncio.TimeoutError as error:
                    elapsed = time.monotonic() - action_started
                    raise AssertionError(
                        f"Action '{action.action_type}' timed out after {elapsed:.2f}s "
                        f"(index {index})"
                    ) from error
                outcome.actions.append(result)
            outcome.duration_seconds = time.monotonic() - start
        except AssertionError as error:
            outcome.result = ScenarioResult.FAILED
            outcome.error = str(error)
            outcome.duration_seconds = time.monotonic() - start
        except Exception as error:
            outcome.result = ScenarioResult.FAILED
            outcome.error = str(error)
            outcome.duration_seconds = time.monotonic() - start
        finally:
            await runtime.stop_cluster()
        return outcome
