import json
import os
from urllib.parse import urlparse

from hyperscale.cli.exceptions.graph.sync import NotSetError
from hyperscale.logging import HyperscaleLogger, LoggerTypes, logging_manager
from hyperscale.projects.management import GraphManager
from hyperscale.projects.management.graphs.actions import RepoConfig


def get_project(
    url: str, 
    path: str,
    branch: str, 
    remote: str, 
    username: str, 
    password: str,
    bypass_connection_validation: bool,
    connection_validation_retries: int,
    log_level: str
):
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
    
    logger['console'].sync.info(f'Fetching project at - {url} - and saving at - {path}...')

    hyperscale_config_filepath = os.path.join(
        path,
        '.hyperscale.json'
    )

    hyperscale_config = {}
    if os.path.exists(hyperscale_config_filepath):
        with open(hyperscale_config_filepath, 'r') as hyperscale_config_file:
            hyperscale_config = json.load(hyperscale_config_file)

    hyperscale_project_config = hyperscale_config.get('project', {})


    if os.path.exists(path) is False:
        os.mkdir(path)

    if url is None:
        url = hyperscale_project_config.get('project_url')
    
    if username is None:
        username = hyperscale_project_config.get('project_username', "")

    if password is None:
        password = hyperscale_project_config.get('project_password', "")

    if branch is None:
        branch = hyperscale_project_config.get('project_branch', 'main')

    if remote is None:
        remote = hyperscale_project_config.get('project_remote', 'origin')

    if url is None:
        raise NotSetError(
            path,
            'url',
            '--url'
        )

    parsed_url = urlparse(url)
    repo_url = f'{parsed_url.scheme}://{username}:{password}@{parsed_url.hostname}{parsed_url.path}'

    logger['console'].sync.info('Initializing project manager.')

    repo_config = RepoConfig(
        path,
        repo_url,
        branch=branch,
        remote=remote,
        username=username,
        password=password
    )
    
    manager = GraphManager(repo_config, log_level=log_level)
    workflow_actions = [
            'fetch'
        ]

    manager.execute_workflow(workflow_actions)

    hyperscale_project_config.update({
        'project_url': url,
        'project_username': username,
        'project_password': password,
        'project_branch': branch,
        'project_remote': remote
    })

    discovered = manager.discover_graph_files()
    hyperscale_config.update(discovered)

    new_graphs_count = len({
        graph_name for graph_name in discovered['graphs'] if graph_name not in hyperscale_config['graphs']
    })

    new_plugins_count = len(({
        plugin_name for plugin_name in discovered['plugins'] if plugin_name not in hyperscale_config['plugins']
    }))

    logger['console'].sync.info(f'Found - {new_graphs_count} - new graphs and - {new_plugins_count} - plugins.')


    logger['console'].sync.info('Saving project state to .hyperscale.json config.')

    hyperscale_config['project'] = hyperscale_project_config
    hyperscale_config['core'] = {
        "bypass_connection_validation": bypass_connection_validation,
        "connection_validation_retries": connection_validation_retries
    }

    with open(hyperscale_config_filepath, 'w') as hyperscale_config_file:
            hyperscale_config = json.dump(hyperscale_config, hyperscale_config_file, indent=4)
            
    logger['console'].sync.info('Project fetch complete!\n')