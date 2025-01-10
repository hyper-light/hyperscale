from hyperscale.graph import Workflow
from .cli import CLI, ImportFile


@CLI.group()
async def run():
    '''
    Commands for running tests either locally or
    against a remote (cloud) instance.
    '''


@run.command()
async def test(
    test_file: ImportFile[Workflow]
):
    '''
    Run the specified test file locally
    '''

    print(test_file.data)

@run.command()
async def job(
    job_name: str
):
    '''
    Run the specified job on the specified remote Hyperscale
    installation.
    '''