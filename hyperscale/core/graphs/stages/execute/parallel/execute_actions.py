import asyncio
import gc
import os
import pickle
import signal
import threading
import time
import warnings
from collections import defaultdict
from typing import Any, Dict, List, Type, Union

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
from hyperscale.core.graphs.stages.base.parallel.partition_method import PartitionMethod
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.setup.setup import Setup
from hyperscale.core.graphs.stages.types.stage_types import StageTypes
from hyperscale.core.hooks.types.action.hook import ActionHook
from hyperscale.core.hooks.types.base.event_graph import EventGraph
from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.base.registrar import registrar
from hyperscale.core.hooks.types.base.simple_context import SimpleContext
from hyperscale.core.hooks.types.load.hook import LoadHook
from hyperscale.core.hooks.types.task.hook import TaskHook
from hyperscale.core.personas import get_persona
from hyperscale.core.personas.persona_registry import registered_personas
from hyperscale.data.serializers.serializer import Serializer
from hyperscale.logging.hyperscale_logger import (
    HyperscaleLogger,
    LoggerTypes,
    logging_manager,
)
from hyperscale.plugins.types.engine.engine_plugin import EnginePlugin
from hyperscale.plugins.types.extension.extension_plugin import ExtensionPlugin
from hyperscale.plugins.types.persona.persona_plugin import PersonaPlugin
from hyperscale.plugins.types.plugin_types import PluginType
from hyperscale.reporting.reporter import ReporterConfig
from hyperscale.versioning.flags.types.base.active import active_flags
from hyperscale.versioning.flags.types.base.flag_type import FlagTypes

warnings.simplefilter("ignore")
warnings.filterwarnings("ignore")


async def start_execution(
    metadata_string: str = None,
    persona_config: Config = None,
    setup_stage: Setup = None,
    workers: int = None,
    worker_id: int = None,
    source_stage_name: str = None,
    source_stage_stream_configs: List[ReporterConfig] = [],
    logfiles_directory: str = None,
    log_level: str = None,
    extensions: Dict[str, ExtensionPlugin] = {},
    loaded_actions: List[str] = [],
) -> Dict[str, Any]:
    current_task = asyncio.current_task()

    logging_manager.disable(
        LoggerTypes.DISTRIBUTED, LoggerTypes.DISTRIBUTED_FILESYSTEM, LoggerTypes.SPINNER
    )

    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory

    logger = HyperscaleLogger()
    logger.logger_directory = logfiles_directory
    logger.initialize()
    await logger.filesystem.aio.create_logfile("hyperscale.core.log")
    logger.filesystem.create_filelogger("hyperscale.core.log")

    start = time.monotonic()

    await setup_stage.run_internal()

    stages: Dict[str, Stage] = setup_stage.context["setup_stage_ready_stages"]

    setup_execute_stage: Stage = stages.get(source_stage_name)
    setup_execute_stage.logger = logger

    persona = get_persona(persona_config)
    persona.stream_reporter_configs = source_stage_stream_configs
    persona.workers = workers

    actions_and_tasks: List[Union[ActionHook, TaskHook]] = []

    serializer = Serializer()
    if len(loaded_actions) > 0:
        for serialized_action in loaded_actions:
            actions_and_tasks.append(serializer.deserialize_action(serialized_action))

    actions_and_tasks.extend(
        setup_stage.context["execute_stage_setup_hooks"].get(source_stage_name, [])
    )

    execution_hooks_count = len(actions_and_tasks)
    await logger.filesystem.aio["hyperscale.core"].info(
        f"{metadata_string} - Executing {execution_hooks_count} actions with a batch size of {persona_config.batch_size} for {persona_config.total_time} seconds using Persona - {persona.type.capitalize()}"
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

    for load_hook in setup_execute_stage.hooks[HookType.LOAD]:
        load_hook: LoadHook = load_hook
        load_hook.parser_config = setup_stage.config

    pipeline_stages = {setup_execute_stage.name: setup_execute_stage}

    hooks_by_type: Dict[HookType, Union[List[ActionHook], List[TaskHook]]] = (
        defaultdict(list)
    )

    for hook in actions_and_tasks:
        hooks_by_type[hook.hook_type].append(hook)

    persona.setup(hooks_by_type, metadata_string)

    persona.cpu_monitor.stage_type = StageTypes.EXECUTE
    persona.memory_monitor.stage_type = StageTypes.EXECUTE

    await logger.filesystem.aio["hyperscale.core"].info(
        f"{metadata_string} - Starting execution"
    )

    results = await persona.execute()

    elapsed = time.monotonic() - start

    await logger.filesystem.aio["hyperscale.core"].info(
        f"{metadata_string} - Execution complete - Time (including addtional setup) took: {round(elapsed, 2)} seconds"
    )

    context = {}

    for stage in pipeline_stages.values():
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

    for idx, result in enumerate(results):
        results[idx] = dill.dumps(result)

    results_dict = {
        "worker_idx": worker_id,
        "streamed_analytics": persona.streamed_analytics,
        "results": results,
        "total_results": len(results),
        "total_elapsed": persona.total_elapsed,
        "context": context,
        "monitoring": {
            "memory": persona.memory_monitor.collected,
            "cpu": persona.cpu_monitor.collected,
        },
    }

    pending_tasks = [task for task in asyncio.all_tasks() if task != current_task]

    for task in pending_tasks:
        if not task.cancelled():
            task.cancel()

    return results_dict


def execute_actions(parallel_config: str):
    import asyncio

    import uvloop

    uvloop.install()

    from hyperscale.logging import logging_manager

    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    def handle_loop_stop(signame):
        try:
            pending_events = asyncio.all_tasks()

            for task in pending_events:
                if not task.cancelled():
                    try:
                        task.cancel()
                    except asyncio.CancelledError:
                        pass
                    except asyncio.InvalidStateError:
                        pass

            loop.close()
            gc.collect()

        except BrokenPipeError:
            pass

        except RuntimeError:
            pass

    for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
        loop.add_signal_handler(
            getattr(signal, signame), lambda signame=signame: handle_loop_stop(signame)
        )

    try:
        parallel_config: Dict[str, Any] = dill.loads(parallel_config)

        graph_name = parallel_config.get("graph_name")
        graph_path: str = parallel_config.get("graph_path")
        graph_id = parallel_config.get("graph_id")
        enable_unstable_features = parallel_config.get(
            "enable_unstable_features", False
        )

        logfiles_directory = parallel_config.get("logfiles_directory")
        log_level = parallel_config.get("log_level")
        source_stage_name = parallel_config.get("source_stage_name")
        source_stage_context: Dict[str, Any] = parallel_config.get(
            "source_stage_context"
        )
        source_stage_plugins = parallel_config.get("source_stage_plugins")
        source_stage_config: Config = parallel_config.get("source_stage_config")
        source_stage_id = parallel_config.get("source_stage_id")
        source_setup_stage_name = parallel_config.get("source_setup_stage_name")
        source_stage_stream_configs = parallel_config.get("source_stage_stream_configs")
        source_stage_loaded_actions = parallel_config.get("source_stage_loaded_actions")
        partition_method = parallel_config.get("partition_method")
        worker_id = parallel_config.get("worker_id")
        workers = parallel_config.get("workers")

        thread_id = threading.current_thread().ident
        process_id = os.getpid()

        active_flags[FlagTypes.UNSTABLE_FEATURE] = enable_unstable_features

        logging_manager.disable(
            LoggerTypes.DISTRIBUTED,
            LoggerTypes.DISTRIBUTED_FILESYSTEM,
            LoggerTypes.SPINNER,
        )

        logging_manager.update_log_level(log_level)
        logging_manager.logfiles_directory = logfiles_directory

        metadata_string = f"Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - "

        discovered: Dict[str, Stage] = import_stages(graph_path)
        plugins_by_type = import_plugins(graph_path)

        initialized_stages = {}
        hooks_by_type = defaultdict(dict)
        hooks_by_name = {}
        hooks_by_shortname = defaultdict(dict)

        generated_hooks = {}
        for stage in discovered.values():
            stage: Stage = stage()

            if stage.context is None:
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
                    hook.stage_instance = initialized_stage
                    hooks_by_type[hook_type][hook.name] = hook
                    hooks_by_name[hook.name] = hook
                    hooks_by_shortname[hook_type][hook.shortname] = hook

            initialized_stages[initialized_stage.name] = initialized_stage

        execute_stage: Stage = initialized_stages.get(source_stage_name)
        execute_stage.context.update(source_stage_context)

        setup_stage: Setup = initialized_stages.get(source_setup_stage_name)
        setup_stage.context.update(source_stage_context)

        if (
            partition_method == PartitionMethod.BATCHES
            and source_stage_config.optimized is False
        ):
            if workers == worker_id:
                source_stage_config.batch_size = int(
                    source_stage_config.batch_size / workers
                ) + (source_stage_config.batch_size % workers)

                if source_stage_config.experiment:
                    distribution = source_stage_config.experiment.get("distribution")

                    for idx, distribution_value in enumerate(distribution):
                        distribution[idx] = int(distribution_value / workers) + (
                            distribution_value % workers
                        )

                    source_stage_config.experiment["distribution"] = distribution

            else:
                source_stage_config.batch_size = int(
                    source_stage_config.batch_size / workers
                )

                if source_stage_config.experiment:
                    distribution = source_stage_config.experiment.get("distribution")

                    for idx, distribution_value in enumerate(distribution):
                        distribution[idx] = int(distribution_value / workers)

                    source_stage_config.experiment["distribution"] = distribution

        stage_persona_plugins: List[str] = source_stage_plugins[PluginType.PERSONA]
        persona_plugins: Dict[str, Type[PersonaPlugin]] = plugins_by_type[
            PluginType.PERSONA
        ]

        stage_engine_plugins: List[str] = source_stage_plugins[PluginType.ENGINE]
        engine_plugins: Dict[str, Type[EnginePlugin]] = plugins_by_type[
            PluginType.ENGINE
        ]

        stage_extension_plugins: List[str] = source_stage_plugins[PluginType.EXTENSION]

        enabled_extensions: Dict[str, ExtensionPlugin] = {}

        for plugin_name in stage_persona_plugins:
            plugin = persona_plugins.get(plugin_name)
            plugin.name = plugin_name
            registered_personas[plugin_name] = lambda config: plugin(config)

        for plugin_name in stage_engine_plugins:
            plugin = engine_plugins.get(plugin_name)
            plugin.name = plugin_name
            registered_engines[plugin_name] = lambda config: plugin(config)

        events_graph = EventGraph(hooks_by_type)
        events_graph.hooks_to_events().assemble_graph().apply_graph_to_events()

        for stage in initialized_stages.values():
            stage.dispatcher.assemble_action_and_task_subgraphs()

        if setup_stage.context is None:
            setup_stage.context = SimpleContext()

        setup_stage.config = source_stage_config
        setup_stage.generation_setup_candidates = 1
        setup_stage.config = source_stage_config
        setup_stage.context["setup_stage_is_primary_thread"] = False
        setup_stage.context["setup_stage_target_config"] = source_stage_config
        setup_stage.context["setup_stage_target_stages"] = {
            execute_stage.name: execute_stage
        }

        results = loop.run_until_complete(
            start_execution(
                metadata_string=metadata_string,
                persona_config=source_stage_config,
                setup_stage=setup_stage,
                workers=workers,
                worker_id=worker_id,
                source_stage_name=source_stage_name,
                source_stage_stream_configs=source_stage_stream_configs,
                logfiles_directory=logfiles_directory,
                log_level=log_level,
                extensions=enabled_extensions,
                loaded_actions=source_stage_loaded_actions,
            )
        )

        loop.close()
        gc.collect()

        return results

    except BrokenPipeError:
        raise ProcessKilledError()

    except RuntimeError:
        raise ProcessKilledError()

    except Exception as e:
        raise e
