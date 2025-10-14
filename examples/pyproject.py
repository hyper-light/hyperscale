import asyncio
from hyperscale.commands.cli.help_message.project import find_caller_relative_path_to_pyproject
from hyperscale.commands.cli.help_message.project.pyproject_toml import PyProjectToml



async def test():
    project = await find_caller_relative_path_to_pyproject()
    project = PyProjectToml(**project)


asyncio.run(test())