import json
import os
from urllib.parse import urlparse

from hyperscale.cli.exceptions.graph.sync import NotSetError
from hyperscale.logging.hyperscale_logger import (
    HyperscaleLogger,
    LoggerTypes,
    logging_manager,
)
from hyperscale.projects.management import GraphManager
from hyperscale.projects.management.graphs.actions import RepoConfig


def sync_project(
    url: str,
    path: str,
    branch: str,
    remote: str,
    sync_message: str,
    username: str,
    password: str,
    ignore: str,
    log_level: str,
    local: bool,
):
    logging_manager.disable(
        LoggerTypes.HYPERSCALE,
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.FILESYSTEM,
        LoggerTypes.DISTRIBUTED_FILESYSTEM,
    )

    logging_manager.update_log_level(log_level)

    logger = HyperscaleLogger()
    logger.initialize()
    logging_manager.logfiles_directory = os.getcwd()

    logger["console"].sync.info(f"Running project sync at - {path}...")

    hyperscale_config_filepath = os.path.join(path, ".hyperscale.json")

    hyperscale_config = {}
    if os.path.exists(hyperscale_config_filepath):
        with open(hyperscale_config_filepath, "r") as hyperscale_config_file:
            hyperscale_config = json.load(hyperscale_config_file)

    hyperscale_project_config = hyperscale_config.get("project", {})

    if url is None:
        url = hyperscale_project_config.get("project_url")

    if username is None:
        username = hyperscale_project_config.get("project_username", "")

    if password is None:
        password = hyperscale_project_config.get("project_password", "")

    if branch is None:
        branch = hyperscale_project_config.get("project_branch", "main")

    if remote is None:
        remote = hyperscale_project_config.get("project_remote", "origin")

    if url is None:
        raise NotSetError(path, "url", "--url")

    parsed_url = urlparse(url)
    repo_url = f"{parsed_url.scheme}://{username}:{password}@{parsed_url.hostname}{parsed_url.path}"

    logger["console"].sync.info("Initializing project manager.")

    repo_config = RepoConfig(
        path,
        repo_url,
        branch=branch,
        remote=remote,
        sync_message=sync_message,
        username=username,
        password=password,
        ignore_options=ignore,
    )

    manager = GraphManager(repo_config, log_level=log_level)
    discovered = manager.discover_graph_files()

    new_graphs_count = len(
        {
            graph_name
            for graph_name in discovered["graphs"]
            if graph_name not in hyperscale_config["graphs"]
        }
    )

    new_plugins_count = len(
        (
            {
                plugin_name
                for plugin_name in discovered["plugins"]
                if plugin_name not in hyperscale_config["plugins"]
            }
        )
    )

    logger["console"].sync.info(
        f"Found - {new_graphs_count} - new graphs and - {new_plugins_count} - plugins."
    )

    if local is False:
        logger["console"].sync.info(
            f"Synchronizing project state with remote at - {url}..."
        )

        workflow_actions = ["initialize", "synchronize"]

        manager.execute_workflow(workflow_actions)

    else:
        logger["console"].sync.info("Skipping remote sync.")

    hyperscale_project_config.update(
        {
            "project_url": url,
            "project_username": username,
            "project_password": password,
            "project_branch": branch,
            "project_remote": remote,
        }
    )

    hyperscale_config.update(discovered)

    logger["console"].sync.info("Saving project state to .hyperscale.json config.")

    hyperscale_config["project"] = hyperscale_project_config
    with open(hyperscale_config_filepath, "w") as hyperscale_config_file:
        hyperscale_config = json.dump(
            hyperscale_config, hyperscale_config_file, indent=4
        )

    logger["console"].sync.info("Sync complete!\n")
