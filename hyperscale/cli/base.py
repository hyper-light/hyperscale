import os
from importlib.metadata import version
from typing import Any, Callable, List, Optional, Union

import click
from art import text2art




class CLI(click.MultiCommand):
    command_files = {
        "ping": "ping.py",
        "graph": "graph.py",
        "project": "project.py",
        "cloud": "cloud.py",
        "plugin": "plugin.py",
    }
    logger = HyperscaleLogger()

    def __init__(
        self,
        name: Optional[str] = None,
        invoke_without_command: bool = False,
        no_args_is_help: Optional[bool] = None,
        subcommand_metavar: Optional[str] = None,
        chain: bool = False,
        result_callback: Optional[Callable[..., Any]] = None,
        **attrs: Any,
    ) -> None:
        super().__init__(
            name,
            invoke_without_command,
            no_args_is_help,
            subcommand_metavar,
            chain,
            result_callback,
            **attrs,
        )

        self.logger.initialize()

        header_text = text2art("Hyperscale", font="alligator").strip("\n")
        hyperscale_version = version("hyperscale")

        self.logger.console.sync.info(f"\n{header_text} {hyperscale_version}\n\n")

    def list_commands(self, ctx: click.Context) -> List[str]:
        rv = []
        for filename in self.command_files.values():
            rv.append(filename[:-3])
        rv.sort()
        return rv

    def get_command(self, ctx: click.Context, name: str) -> Union[click.Command, None]:
        ns = {}

        command_file = os.path.join(
            os.path.dirname(__file__), self.command_files.get(name, "ping.py")
        )

        with open(command_file) as f:
            code = compile(f.read(), command_file, "exec")
            eval(code, ns, ns)

        return ns.get(name)
