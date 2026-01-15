import importlib
import inspect
from typing import Any
import typing

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks.step import step
from hyperscale.core.state import Provide, Use, state
from hyperscale.core.engines.client.http.models.http.http_response import HTTPResponse
from hyperscale.core.engines.client.shared.models.url import URL


class DynamicWorkflowFactory:
    def __init__(self, workflow_registry: dict[str, type[Workflow]]) -> None:
        self._workflow_registry = workflow_registry
        self._type_registry: dict[str, type] = {
            "HTTPResponse": HTTPResponse,
            "URL": URL,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "dict": dict,
            "list": list,
        }

    def build_workflows(
        self, workflow_specs: list[dict[str, Any]]
    ) -> list[tuple[list[str], object]]:
        workflows: list[tuple[list[str], object]] = []
        for index, workflow_spec in enumerate(workflow_specs):
            workflow_name = workflow_spec.get("name")
            if not workflow_name:
                raise ValueError("workflow_instances requires name")
            workflow_class = self._resolve_workflow_class(workflow_name)
            subclass_name = workflow_spec.get(
                "subclass_name", f"{workflow_name}Dynamic{index}"
            )
            class_overrides = workflow_spec.get("class_overrides", {})
            step_specs = workflow_spec.get("steps", [])
            state_specs = workflow_spec.get("states", [])
            workflow_class = self._build_subclass(
                workflow_class,
                subclass_name,
                class_overrides,
                step_specs,
                state_specs,
            )
            init_kwargs = workflow_spec.get("init", {})
            workflow_instance = workflow_class(**init_kwargs)
            dependencies = workflow_spec.get("depends_on", [])
            if isinstance(dependencies, str):
                dependencies = [dependencies]
            workflows.append((dependencies, workflow_instance))
        return workflows

    def _resolve_workflow_class(self, name: str) -> type[Workflow]:
        if name not in self._workflow_registry:
            raise ValueError(f"Unknown workflow '{name}'")
        return self._workflow_registry[name]

    def _build_subclass(
        self,
        base_class: type[Workflow],
        subclass_name: str,
        class_overrides: dict[str, Any],
        step_specs: list[dict[str, Any]],
        state_specs: list[dict[str, Any]],
    ) -> type[Workflow]:
        class_attrs: dict[str, Any] = {"__module__": base_class.__module__}
        class_attrs.update(class_overrides)
        for step_spec in step_specs:
            hook = self._build_step_hook(subclass_name, step_spec)
            class_attrs[hook.name] = hook
        for state_spec in state_specs:
            hook = self._build_state_hook(subclass_name, state_spec)
            class_attrs[hook.name] = hook
        return type(subclass_name, (base_class,), class_attrs)

    def _build_step_hook(self, subclass_name: str, step_spec: dict[str, Any]):
        step_name = step_spec.get("name")
        if not step_name:
            raise ValueError("step spec requires name")
        client_name = step_spec.get("client")
        method_name = step_spec.get("method")
        if client_name is None and method_name is None:
            return_value = step_spec.get("return_value")
        else:
            return_value = None
        return_type = self._resolve_type(step_spec.get("return_type", "object"))
        dependencies = step_spec.get("depends_on", [])
        if isinstance(dependencies, str):
            dependencies = [dependencies]
        parameters, annotations = self._build_parameters(step_spec.get("params", []))
        factory = self

        async def dynamic_step(self, **kwargs):
            resolved_args = factory._resolve_value_list(
                step_spec.get("args", []), kwargs
            )
            resolved_kwargs = factory._resolve_value_map(
                step_spec.get("kwargs", {}), kwargs
            )
            if return_value is not None:
                return factory._resolve_value(return_value, kwargs)
            if client_name is None or method_name is None:
                raise ValueError(f"Step '{step_name}' requires client and method")
            client_name_value = str(client_name)
            method_name_value = str(method_name)
            client = getattr(self.client, client_name_value)
            method = getattr(client, method_name_value)
            return await method(*resolved_args, **resolved_kwargs)

        self._apply_function_metadata(
            dynamic_step,
            subclass_name,
            step_name,
            return_type,
            parameters,
            annotations,
        )
        return step(*dependencies)(dynamic_step)

    def _build_state_hook(self, subclass_name: str, state_spec: dict[str, Any]):
        state_name = state_spec.get("name")
        if not state_name:
            raise ValueError("state spec requires name")
        workflows = state_spec.get("workflows", [])
        if isinstance(workflows, str):
            workflows = [workflows]
        mode = state_spec.get("mode", "provide")
        value = state_spec.get("value")
        parameters, annotations = self._build_parameters(state_spec.get("params", []))
        state_type = self._resolve_type(state_spec.get("state_type", "object"))
        return_type = Provide[state_type] if mode == "provide" else Use[state_type]
        source = state_spec.get("source")
        factory = self

        async def dynamic_state(self, **kwargs) -> Use[object] | Provide[object]:
            if value is not None:
                return typing.cast(
                    Use[object] | Provide[object],
                    factory._resolve_value(value, kwargs),
                )
            if source:
                return typing.cast(Use[object] | Provide[object], kwargs.get(source))
            if parameters:
                return typing.cast(
                    Use[object] | Provide[object],
                    kwargs.get(parameters[0].name),
                )
            return typing.cast(Use[object] | Provide[object], None)

        self._apply_function_metadata(
            dynamic_state,
            subclass_name,
            state_name,
            return_type,
            parameters,
            annotations,
        )
        state_callable = cast(
            Callable[..., Awaitable[Use[object] | Provide[object]]],
            dynamic_state,
        )
        return state(*workflows)(state_callable)

    def _build_parameters(
        self, param_specs: list[dict[str, Any]]
    ) -> tuple[list[inspect.Parameter], dict[str, type]]:
        parameters: list[inspect.Parameter] = []
        annotations: dict[str, type] = {}
        for spec in param_specs:
            name = spec.get("name")
            if not name:
                raise ValueError("parameter spec requires name")
            default = spec.get("default", inspect._empty)
            parameter_type = spec.get("type")
            if parameter_type is not None:
                annotations[name] = self._resolve_type(parameter_type)
            parameters.append(
                inspect.Parameter(
                    name,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=default,
                )
            )
        return parameters, annotations

    def _apply_function_metadata(
        self,
        func,
        subclass_name: str,
        func_name: str,
        return_type: type,
        parameters: list[inspect.Parameter],
        annotations: dict[str, type],
    ) -> None:
        func.__name__ = func_name
        func.__qualname__ = f"{subclass_name}.{func_name}"
        func.__annotations__ = {"return": return_type, **annotations}
        signature_parameters = [
            inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            *parameters,
            inspect.Parameter("kwargs", inspect.Parameter.VAR_KEYWORD),
        ]
        func.__signature__ = inspect.Signature(signature_parameters)

    def _resolve_type(self, type_name: Any) -> type:
        if isinstance(type_name, type):
            return type_name
        if not isinstance(type_name, str):
            raise ValueError(f"Invalid type reference {type_name}")
        if type_name in self._type_registry:
            return self._type_registry[type_name]
        if "." in type_name:
            module_name, attr_name = type_name.rsplit(".", 1)
            module = importlib.import_module(module_name)
            return getattr(module, attr_name)
        return object

    def _resolve_value(self, value: Any, context: dict[str, Any]) -> Any:
        if isinstance(value, dict) and "context" in value:
            return context.get(value["context"])
        if isinstance(value, list):
            return [self._resolve_value(item, context) for item in value]
        if isinstance(value, dict):
            return {
                key: self._resolve_value(val, context) for key, val in value.items()
            }
        return value

    def _resolve_value_list(
        self, values: list[Any], context: dict[str, Any]
    ) -> list[Any]:
        return [self._resolve_value(item, context) for item in values]

    def _resolve_value_map(
        self, values: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        return {key: self._resolve_value(val, context) for key, val in values.items()}
