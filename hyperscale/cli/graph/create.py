import inspect
import json
import os
from pathlib import Path
from typing import Optional

from hyperscale.cli.exceptions.graph.create import InvalidStageType
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.logging import HyperscaleLogger, LoggerTypes, logging_manager
from hyperscale.projects.generation import GraphGenerator


def create_graph(
    path: str, 
    stages: Optional[str], 
    engine: str,
    reporter: str,
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

    logger['console'].sync.info(f'Creating new graph at - {path}.')

    if stages is None:
        stages_list = [
            'setup',
            'execute',
            'analyze',
            'submit'
        ]

    else:
        stages_list = stages.split(',')
    
    generated_stages_count = len(stages_list)
    generated_stages = ''.join([
        f'\n-{stage}' for stage in stages_list
    ])   

    logger['console'].sync.info(f'Generating - {generated_stages_count} stages:{generated_stages}') 


    generator = GraphGenerator()

    for stage in stages_list:
        if stage not in generator.valid_types:
            raise InvalidStageType(stage, [
                generator_type_name for generator_type_name, generator_type in generator.generator_types.items() if inspect.isclass(
                    generator_type
                ) and issubclass(generator_type, Stage)
            ])


    with open(path, 'w') as generated_test:
        generated_test.write(
            generator.generate_graph(
                stages_list,
                engine=engine,
                reporter=reporter
            )
        )

    graph_name = path
    if os.path.isfile(graph_name):
        graph_name = Path(graph_name).stem

    hyperscale_config_filepath = os.path.join(
        os.getcwd(),
        '.hyperscale.json'
    )

    hyperscale_config = {}
    if os.path.exists(hyperscale_config_filepath):
        with open(hyperscale_config_filepath, 'r') as hyperscale_config_file:
            hyperscale_config = json.load(hyperscale_config_file)

    hyperscale_graphs = hyperscale_config.get('graphs', {})

    if hyperscale_graphs.get(graph_name) is None:
        hyperscale_graphs[graph_name] = str(Path(path).absolute().resolve())
        with open(hyperscale_config_filepath, 'w') as hyperscale_config_file:
            hyperscale_config['graphs'] = hyperscale_graphs
            json.dump(hyperscale_config, hyperscale_config_file, indent=4)   

    logger['console'].sync.info('\nGraph generated!\n')