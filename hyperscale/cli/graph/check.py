import asyncio
import importlib
import inspect
import json
import ntpath
import os
import sys
from pathlib import Path

import uvloop

from hyperscale.core.graphs.graph import Graph
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.logging import HyperscaleLogger, LoggerTypes, logging_manager

uvloop.install()


def check_graph(path: str, log_level: str):

    logging_manager.disable(
        LoggerTypes.HYPERSCALE, 
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.FILESYSTEM,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )
    logging_manager.update_log_level(log_level)

    logger = HyperscaleLogger()
    logger.initialize()
    logging_manager.logfiles_directory = os.getcwd()

    graph_name = path
    if os.path.isfile(graph_name):
        graph_name = Path(graph_name).stem

    logger['console'].sync.info(f'Validating graph - {graph_name} - at - {path}.\n')

    hyperscale_config_filepath = os.path.join(
        os.getcwd(),
        '.hyperscale.json'
    )

    hyperscale_config = {}
    if os.path.exists(hyperscale_config_filepath):
        with open(hyperscale_config_filepath, 'r') as hyperscale_config_file:
            hyperscale_config = json.load(hyperscale_config_file)

    hyperscale_graphs = hyperscale_config.get('graphs', {})
    hyperscale_core_config = hyperscale_config.get('core', {
        'bypass_connection_validation': False,
        'connection_validation_retries': 3
    })
    
    if path in hyperscale_graphs:
        path = hyperscale_graphs.get(path)
    
    package_dir = Path(path).resolve().parent
    package_dir_path = str(package_dir)
    package_dir_module = package_dir_path.split('/')[-1]
    
    package = ntpath.basename(path)
    package_slug = package.split('.')[0]
    spec = importlib.util.spec_from_file_location(f'{package_dir_module}.{package_slug}', path)

    if path not in sys.path:
        sys.path.append(str(package_dir.parent))

    module = importlib.util.module_from_spec(spec)
    sys.modules[module.__name__] = module

    spec.loader.exec_module(module)

    logger['console'].sync.info('Loaded graph.')
    
    direct_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())

    discovered = {}
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and issubclass(obj, Stage) and obj not in direct_decendants:
            discovered[name] = obj

    stages_count = len(discovered)

    logger['console'].sync.info(f'Validating - {stages_count} - stages.')

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    graph = Graph(
        graph_name,
        list(discovered.values()),
        config={
            **hyperscale_core_config,
            'graph_module': module.__name__
        },
        cpus=1
    )

    graph.assemble()

    logger['console'].sync.info('\nValidation complete!\n')

    os._exit(0)