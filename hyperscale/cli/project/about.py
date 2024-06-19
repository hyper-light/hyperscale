import json
import os
from typing import Any, Dict

from hyperscale.logging.hyperscale_logger import HyperscaleLogger, LoggerTypes, logging_manager


def about_project(
    path: str
):
    logging_manager.disable(
        LoggerTypes.HYPERSCALE, 
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.FILESYSTEM,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )

    logger = HyperscaleLogger()
    logger.initialize()
    logging_manager.logfiles_directory = os.getcwd()

    hyperscale_config_filepath = os.path.join(
        path,
        '.hyperscale.json'
    )

    hyperscale_config = {}
    if os.path.exists(hyperscale_config_filepath):
        with open(hyperscale_config_filepath, 'r') as config_file:
            hyperscale_config: Dict[str, Any] = json.load(config_file)

    else:
        logger.console.sync.error(f'No project found at path - {path}\n')
        os._exit(0)

    project_name = hyperscale_config.get('name')
    logger.console.sync.info(f'Project - {project_name}')

    logger.console.sync.info('\nGraphs:')
    hyperscale_graphs = hyperscale_config.get('graphs', {})

    if len(hyperscale_graphs) > 0:
        for graph_name, graph_path in hyperscale_graphs.items():
            logger.console.sync.info(f' - {graph_name} at {graph_path}')

    else:
        logger.console.sync.info(' - No graphs registered')

    logger.console.sync.info('\nPlugins:')
    hyperscale_plugins = hyperscale_config.get('plugins', {})

    if len(hyperscale_plugins) > 0:
        for plugin_name, plugin_path in hyperscale_plugins.items():
            logger.console.sync.info(f' - {plugin_name} at {plugin_path}')

    else:
        logger.console.sync.info(' - No plugins registered')


    logger.console.sync.info('\nCore Options:')
    hyperscale_options: Dict[str, Any] = hyperscale_config.get('core', {})
    
    if len(hyperscale_options) > 0:
        for opttion_name, option_value in hyperscale_options.items():
            logger.console.sync.info(f' - {opttion_name} set as {option_value}')

    else:
        logger.console.sync.info(' - No options set')


    logger.console.sync.info('')