import asyncio
import sys

from .arg_types import Context
from .command import Command, create_command
from .group import Group, create_group
from .help_message import CLIStyle
from .inspect_wrapped import inspect_wrapped


class CLI:
    _entrypoint: Group = Group(None, None, "")
    _global_styles: CLIStyle | None = None

    @classmethod
    async def run(cls, args: list[str] | None = None):
        if args is None:
            args = list(sys.argv)

        context = Context()

        if len(args) > 0:
            return await cls._entrypoint.run(args, context)

        loop = asyncio.get_event_loop()

        subcommands: list[str] | None = None
        if len(cls._entrypoint.subgroups) > 0 or len(cls._entrypoint.subcommands) > 0:
            subcommands = list(cls._entrypoint.subgroups.keys())
            subcommands.extend(cls._entrypoint.subcommands.keys())

        help_message_lines = await cls._entrypoint.help_message.to_lines(
            subcommands=subcommands,
            global_styles=cls._global_styles,
        )

        await loop.run_in_executor(
            None,
            sys.stdout.write,
            help_message_lines,
        )

    @classmethod
    def root(
        cls,
        *commands: list[Group | Command],
        global_styles: CLIStyle | None = None,
        shortnames: dict[str, str] | None = None,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command_call):
            if global_styles:
                cls._global_styles = global_styles

            indentation = 0
            if global_styles:
                indentation = global_styles.indentation

            (
                positional_args_map,
                keyword_args_map,
                help_message,
            ) = inspect_wrapped(
                command_call,
                styling=global_styles,
                shortnames=shortnames,
                indentation=indentation,
            )

            cls._entrypoint.update_command(
                command_call.__name__,
                command_call,
                help_message,
                global_styles=global_styles,
                positional_args=positional_args_map,
                keyword_args_map=keyword_args_map,
            )

            for command in commands:
                if isinstance(command, Group):
                    command._global_styles = cls._global_styles
                    cls._entrypoint.subgroups[command.group_name] = command

                elif isinstance(command, Command):
                    command._global_styles = cls._global_styles
                    cls._entrypoint.subcommands[command.command_name] = command

            return cls

        return wrap

    @classmethod
    def group(
        cls,
        styling: CLIStyle | None = None,
        shortnames: dict[str, str] | None = None,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command):
            if cls._entrypoint.source:
                return cls._entrypoint.group(
                    styling=styling,
                    shortnames=shortnames,
                )(command)

            else:
                return create_group(
                    command,
                    styling=styling,
                    shortnames=shortnames,
                )

        return wrap

    @classmethod
    def command(
        cls,
        styling: CLIStyle | None = None,
        shortnames: dict[str, str] | None = None,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command):
            if cls._entrypoint.source:
                cls._entrypoint.command(styling=styling, shortnames=shortnames)(command)

            return create_command(
                command,
                styling=styling,
                shortnames=shortnames,
            )

        return wrap
