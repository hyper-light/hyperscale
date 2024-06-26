import os
import pickle
import threading
from collections import defaultdict
from typing import Any, Dict, List, Union

import dill

from hyperscale.core.engines.client.config import Config
from hyperscale.core.engines.types.playwright import (
    ContextConfig,
    MercuryPlaywrightClient,
)
from hyperscale.core.engines.types.registry import RequestTypes, registered_engines
from hyperscale.core.graphs.stages.base.exceptions.process_killed_error import (
    ProcessKilledError,
)
from hyperscale.core.graphs.stages.base.import_tools import (
    import_plugins,
    import_stages,
    set_stage_hooks,
)
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.execute import Execute
from hyperscale.core.graphs.stages.optimize.optimization import (
    DistributionFitOptimizer,
    Optimizer,
)
from hyperscale.core.graphs.stages.optimize.optimization.algorithms import (
    registered_algorithms,
)
from hyperscale.core.graphs.stages.setup.setup import Setup
from hyperscale.core.graphs.stages.types.stage_types import StageTypes
from hyperscale.core.hooks.types.action.hook import ActionHook
from hyperscale.core.hooks.types.base.event_graph import EventGraph
from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.base.registrar import registrar
from hyperscale.core.hooks.types.base.simple_context import SimpleContext
from hyperscale.core.hooks.types.load.hook import LoadHook
from hyperscale.core.hooks.types.task.hook import TaskHook
from hyperscale.core.personas.persona_registry import registered_personas
from hyperscale.core.personas.streaming.stream_analytics import StreamAnalytics
from hyperscale.data.serializers.serializer import Serializer
from hyperscale.logging.hyperscale_logger import HyperscaleLogger, LoggerTypes
from hyperscale.monitoring import CPUMonitor, MemoryMonitor
from hyperscale.plugins.types.engine.engine_plugin import EnginePlugin
from hyperscale.plugins.types.persona.persona_plugin import PersonaPlugin
from hyperscale.plugins.types.plugin_types import PluginType
from hyperscale.versioning.flags.types.base.active import active_flags
from hyperscale.versioning.flags.types.base.flag_type import FlagTypes

HooksByType = Dict[HookType, Union[List[ActionHook], List[TaskHook]]]


async def setup_action_channels_and_playwright(
    setup_stage: Setup = None,
    execute_stage: Execute = None,
    logger: HyperscaleLogger = None,
    metadata_string: str = None,
    persona_config: Config = None,
    loaded_actions: List[str] = [],
) -> Execute:
    setup_stage.context = SimpleContext()
    setup_stage.generation_setup_candidates = 1
    setup_stage.context["setup_stage_is_primary_thread"] = False
    setup_stage.context["setup_stage_target_config"] = persona_config
    setup_stage.context["setup_stage_target_stages"] = {
        execute_stage.name: execute_stage
    }

    for event in setup_stage.dispatcher.events_by_name.values():
        event.source.stage_instance = setup_stage
        event.context.update(setup_stage.context)

    await setup_stage.run_internal()

    stages: Dict[str, Stage] = setup_stage.context["setup_stage_ready_stages"]
    setup_execute_stage: Stage = stages.get(execute_stage.name)
    setup_execute_stage.logger = logger

    actions_and_tasks: List[Union[ActionHook, TaskHook]] = []

    serializer = Serializer()
    if len(loaded_actions) > 0:
        for serialized_action in loaded_actions:
            actions_and_tasks.append(serializer.deserialize_action(serialized_action))

    actions_and_tasks.extend(
        setup_stage.context["execute_stage_setup_hooks"].get(execute_stage.name, [])
    )

    for hook in actions_and_tasks:
        if hook.action.type == RequestTypes.PLAYWRIGHT and isinstance(
            hook.session, MercuryPlaywrightClient
        ):
            await logger.filesystem.aio["hyperscale.core"].info(
                f"{metadata_string} - Setting up Playwright Session"
            )

            await logger.filesystem.aio["hyperscale.core"].debug(
                f"{metadata_string} - Playwright Session - {hook.session.session_id} - Browser Type: {persona_config.browser_type}"
            )
            await logger.filesystem.aio["hyperscale.core"].debug(
                f"{metadata_string} - Playwright Session - {hook.session.session_id} - Device Type: {persona_config.device_type}"
            )
            await logger.filesystem.aio["hyperscale.core"].debug(
                f"{metadata_string} - Playwright Session - {hook.session.session_id} - Locale: {persona_config.locale}"
            )
            await logger.filesystem.aio["hyperscale.core"].debug(
                f"{metadata_string} - Playwright Session - {hook.session.session_id} - geolocation: {persona_config.geolocation}"
            )
            await logger.filesystem.aio["hyperscale.core"].debug(
                f"{metadata_string} - Playwright Session - {hook.session.session_id} - Permissions: {persona_config.permissions}"
            )
            await logger.filesystem.aio["hyperscale.core"].debug(
                f"{metadata_string} - Playwright Session - {hook.session.session_id} - Color Scheme: {persona_config.color_scheme}"
            )

            config = hook.session.config
            if config is None:
                config = ContextConfig(
                    browser_type=persona_config.browser_type,
                    device_type=persona_config.device_type,
                    locale=persona_config.locale,
                    geolocation=persona_config.geolocation,
                    permissions=persona_config.permissions,
                    color_scheme=persona_config.color_scheme,
                    options=persona_config.playwright_options,
                )

            await hook.session.setup(config=config)

    hooks_by_type: Dict[HookType, Union[List[ActionHook], List[TaskHook]]] = (
        defaultdict(list)
    )

    for hook in actions_and_tasks:
        hooks_by_type[hook.hook_type].append(hook)

    setup_execute_stage.hooks[HookType.ACTION] = hooks_by_type[HookType.ACTION]
    setup_execute_stage.hooks[HookType.TASK] = hooks_by_type[HookType.TASK]

    for load_hook in setup_execute_stage.hooks[HookType.LOAD]:
        load_hook: LoadHook = load_hook
        load_hook.parser_config = setup_stage.config

    return setup_execute_stage


def optimize_stage(serialized_config: str):
    import asyncio
    import warnings

    import uvloop

    warnings.simplefilter("ignore")

    uvloop.install()

    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    from hyperscale.logging import logging_manager

    try:
        thread_id = threading.current_thread().ident
        process_id = os.getpid()

        optimization_config: Dict[str, Union[str, int, Any]] = dill.loads(
            serialized_config
        )

        graph_name: str = optimization_config.get("graph_name")
        graph_path: str = optimization_config.get("graph_path")
        graph_id: str = optimization_config.get("graph_id")
        logfiles_directory = optimization_config.get("logfiles_directory")
        log_level = optimization_config.get("log_level")
        enable_unstable_features = optimization_config.get(
            "enable_unstable_features", False
        )
        source_stage_name: str = optimization_config.get("source_stage_name")
        worker_id = optimization_config.get("worker_id")

        monitor_name = f"{source_stage_name}.worker"

        cpu_monitor = CPUMonitor()
        memory_monitor = MemoryMonitor()

        cpu_monitor.stage_type = StageTypes.OPTIMIZE
        memory_monitor.stage_type = StageTypes.OPTIMIZE

        cpu_monitor.start_background_monitor_sync(monitor_name)
        memory_monitor.start_background_monitor_sync(monitor_name)

        active_flags[FlagTypes.UNSTABLE_FEATURE] = enable_unstable_features

        logfiles_directory = optimization_config.get("logfiles_directory")
        log_level = optimization_config.get("log_level")

        logging_manager.disable(
            LoggerTypes.DISTRIBUTED, LoggerTypes.DISTRIBUTED_FILESYSTEM
        )

        logging_manager.update_log_level(log_level)
        logging_manager.logfiles_directory = logfiles_directory

        logging_manager.disable(
            LoggerTypes.DISTRIBUTED,
            LoggerTypes.DISTRIBUTED_FILESYSTEM,
            LoggerTypes.SPINNER,
        )

        logging_manager.update_log_level(log_level)
        logging_manager.logfiles_directory = logfiles_directory

        logger = HyperscaleLogger()
        logger.initialize()
        logger.filesystem.sync.create_logfile("hyperscale.core.log")
        logger.filesystem.sync.create_logfile("hyperscale.optimize.log")

        logger.filesystem.create_filelogger("hyperscale.core.log")
        logger.filesystem.create_filelogger("hyperscale.optimize.log")

        source_stage_id: str = optimization_config.get("source_stage_id")
        source_stage_context: Dict[str, Any] = optimization_config.get(
            "source_stage_context"
        )

        execute_stage_name: str = optimization_config.get("execute_stage_name")
        execute_stage_config: Config = optimization_config.get("execute_stage_config")
        execute_setup_stage_name: Config = optimization_config.get(
            "execute_setup_stage_name"
        )
        execute_stage_plugins: Dict[PluginType, List[str]] = optimization_config.get(
            "execute_stage_plugins"
        )
        execute_stage_streamed_analytics: Dict[
            str, Dict[str, List[StreamAnalytics]]
        ] = optimization_config.get("execute_stage_streamed_analytics")

        optimizer_params: List[str] = optimization_config.get("optimizer_params")
        optimizer_iterations: int = optimization_config.get("optimizer_iterations")
        optimizer_algorithm: str = optimization_config.get("optimizer_algorithm")
        optimize_stage_workers: int = optimization_config.get("optimize_stage_workers")
        time_limit: int = optimization_config.get("time_limit")
        batch_size: int = optimization_config.get("execute_stage_batch_size")
        execute_stage_loaded_actions = optimization_config.get(
            "execute_stage_loaded_actions"
        )

        metadata_string = f"Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - "
        discovered: Dict[str, Stage] = import_stages(graph_path)
        plugins_by_type = import_plugins(graph_path)

        initialized_stages = {}
        hooks_by_type = defaultdict(dict)
        hooks_by_name = {}
        hooks_by_shortname = defaultdict(dict)

        execute_stage_config.batch_size = batch_size

        generated_hooks = {}
        for stage in discovered.values():
            stage: Stage = stage()
            stage.context = SimpleContext()
            stage.graph_name = graph_name
            stage.graph_path = graph_path
            stage.graph_id = graph_id

            for hook_shortname, hook in registrar.reserved[stage.name].items():
                hook._call = hook._call.__get__(stage, stage.__class__)
                setattr(stage, hook_shortname, hook._call)

            initialized_stage = set_stage_hooks(stage, generated_hooks)

            for hook_type in initialized_stage.hooks:
                for hook in initialized_stage.hooks[hook_type]:
                    hooks_by_type[hook_type][hook.name] = hook
                    hooks_by_name[hook.name] = hook
                    hooks_by_shortname[hook_type][hook.shortname] = hook

            initialized_stages[initialized_stage.name] = initialized_stage

        execute_stage: Stage = initialized_stages.get(execute_stage_name)
        execute_stage.context.update(source_stage_context)

        setup_stage: Setup = initialized_stages.get(execute_setup_stage_name)
        setup_stage.context.update(source_stage_context)

        stage_persona_plugins: List[str] = execute_stage_plugins[PluginType.PERSONA]
        persona_plugins: Dict[str, PersonaPlugin] = plugins_by_type[PluginType.PERSONA]

        stage_engine_plugins: List[str] = execute_stage_plugins[PluginType.ENGINE]
        engine_plugins: Dict[str, EnginePlugin] = plugins_by_type[PluginType.ENGINE]

        for plugin_name in stage_persona_plugins:
            plugin = persona_plugins.get(plugin_name)
            plugin.name = plugin_name
            registered_personas[plugin_name] = lambda config: plugin(config)

        for plugin_name in stage_engine_plugins:
            plugin = engine_plugins.get(plugin_name)
            plugin.name = plugin_name
            registered_engines[plugin_name] = lambda config: plugin(config)

        for plugin_name, plugin in plugins_by_type[PluginType.OPTIMIZER].items():
            registered_algorithms[plugin_name] = plugin

        events_graph = EventGraph(hooks_by_type)
        events_graph.hooks_to_events().assemble_graph().apply_graph_to_events()

        for stage in initialized_stages.values():
            stage.dispatcher.assemble_action_and_task_subgraphs()

        setup_stage: Setup = initialized_stages.get(execute_setup_stage_name)

        setup_execute_stage: Execute = loop.run_until_complete(
            setup_action_channels_and_playwright(
                setup_stage=setup_stage,
                logger=logger,
                metadata_string=metadata_string,
                persona_config=execute_stage_config,
                execute_stage=execute_stage,
                loaded_actions=execute_stage_loaded_actions,
            )
        )

        pipeline_stages = {setup_execute_stage.name: setup_execute_stage}

        logger.filesystem.sync["hyperscale.optimize"].info(
            f"{metadata_string} - Setting up Optimization"
        )

        if execute_stage_config.experiment and execute_stage_config.experiment.get(
            "distribution"
        ):
            execute_stage_config.experiment["distribution"] = [
                int(distribution_value / optimize_stage_workers)
                for distribution_value in execute_stage_config.experiment.get(
                    "distribution"
                )
            ]

            optimizer = DistributionFitOptimizer(
                {
                    "graph_name": graph_name,
                    "graph_id": graph_id,
                    "source_stage_name": source_stage_name,
                    "source_stage_id": source_stage_id,
                    "params": optimizer_params,
                    "stage_name": execute_stage_name,
                    "stage_config": execute_stage_config,
                    "stage_hooks": setup_execute_stage.hooks,
                    "iterations": optimizer_iterations,
                    "algorithm": optimizer_algorithm,
                    "time_limit": time_limit,
                    "stream_analytics": execute_stage_streamed_analytics,
                }
            )

        else:
            optimizer = Optimizer(
                {
                    "graph_name": graph_name,
                    "graph_id": graph_id,
                    "source_stage_name": source_stage_name,
                    "source_stage_id": source_stage_id,
                    "params": optimizer_params,
                    "stage_name": execute_stage_name,
                    "stage_config": execute_stage_config,
                    "stage_hooks": setup_execute_stage.hooks,
                    "iterations": optimizer_iterations,
                    "algorithm": optimizer_algorithm,
                    "time_limit": time_limit,
                    "stream_analytics": execute_stage_streamed_analytics,
                }
            )

        results = optimizer.optimize()

        if execute_stage_config.experiment and results.get("optimized_distribution"):
            execute_stage_config.experiment["distribution"] = results.get(
                "optimized_distribution"
            )

        execute_stage_config.batch_size = results.get(
            "optimized_batch_size", execute_stage_config.batch_size
        )
        execute_stage_config.batch_interval = results.get(
            "optimized_batch_interval", execute_stage_config.batch_gradient
        )
        execute_stage_config.batch_gradient = results.get(
            "optimized_batch_gradient", execute_stage_config.batch_gradient
        )

        logger.filesystem.sync["hyperscale.optimize"].info(
            f"{optimizer.metadata_string} - Optimization complete"
        )

        context = {}
        for stage in pipeline_stages.values():
            stage.context.ignore_serialization_filters = ["execute_stage_setup_hooks"]

            for key, value in stage.context.as_serializable():
                try:
                    dill.dumps(value)

                except ValueError:
                    stage.context.ignore_serialization_filters.append(key)

                except TypeError:
                    stage.context.ignore_serialization_filters.append(key)

                except pickle.PicklingError:
                    stage.context.ignore_serialization_filters.append(key)

            serializable_context = stage.context.as_serializable()
            context.update(
                {
                    context_key: context_value
                    for context_key, context_value in serializable_context
                }
            )

        cpu_monitor.stop_background_monitor_sync(monitor_name)
        memory_monitor.stop_background_monitor_sync(monitor_name)

        cpu_monitor.close()
        memory_monitor.close()

        loop.close()

        return dill.dumps(
            {
                "worker_id": worker_id,
                "stage": execute_stage.name,
                "config": execute_stage_config,
                "params": results,
                "context": context,
                "monitoring": {
                    "cpu": cpu_monitor.collected,
                    "memory": memory_monitor.collected,
                },
            }
        )

    except KeyboardInterrupt:
        cpu_monitor.stop_background_monitor_sync(monitor_name)
        memory_monitor.stop_background_monitor_sync(monitor_name)

        raise ProcessKilledError()

    except BrokenPipeError:
        cpu_monitor.stop_background_monitor_sync(monitor_name)
        memory_monitor.stop_background_monitor_sync(monitor_name)

        raise ProcessKilledError()

    except RuntimeError:
        cpu_monitor.stop_background_monitor_sync(monitor_name)
        memory_monitor.stop_background_monitor_sync(monitor_name)

        raise ProcessKilledError()

    except Exception as e:
        cpu_monitor.stop_background_monitor_sync(monitor_name)
        memory_monitor.stop_background_monitor_sync(monitor_name)

        raise e
