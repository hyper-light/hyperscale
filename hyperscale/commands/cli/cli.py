import asyncio
import os
import functools
import sys

from .arg_types import Context
from .cli_protocol import CLIProtocol, patch_transport_close
from .command import Command, create_command
from .group import Group, create_group
from .help_message import CLIStyle
from .inspect_wrapped import inspect_wrapped
try:
    import uvloop as uvloop
    has_uvloop = True

except Exception:
    has_uvloop = False


class CLI:
    _entrypoint: Group = Group(None, None, "")
    _global_styles: CLIStyle | None = None

    @classmethod
    async def run(cls, args: list[str] | None = None):
        if args is None:
            args = list(sys.argv)

        context = Context()

        errors: list[str] = []
        exit_code: int | None = None
        if len(args) > 0:
            (
                _,
                errors,
                exit_code,
            ) = await cls._entrypoint.run(args, context)

            await cls._exit(errors, exit_code)
            
        subcommands: list[str] | None = None
        if len(cls._entrypoint.subgroups) > 0 or len(cls._entrypoint.subcommands) > 0:
            subcommands = list(cls._entrypoint.subgroups.keys())
            subcommands.extend(cls._entrypoint.subcommands.keys())

        await cls._print_group_help_message(
            subcommands,
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
        *commands: list[Group | Command],
        styling: CLIStyle | None = None,
        shortnames: dict[str, str] | None = None,
        display_help_on_error: bool = True,
        error_exit_code: int = 1,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command):
            if cls._entrypoint.source:
                return cls._entrypoint.group(
                    styling=styling,
                    shortnames=shortnames,
                    display_help_on_error=display_help_on_error,
                    error_exit_code=error_exit_code,
                )(command)

            else:
                group = create_group(
                    command,
                    styling=styling,
                    shortnames=shortnames,

                )

                for command in commands:
                    if isinstance(command, Group):
                        command._global_styles = cls._global_styles
                        group.subgroups[command.group_name] = command

                        for subcommand in command.subcommands.values():
                            subcommand._global_styles = cls._global_styles

                        for subgroup in command.subgroups.values():
                            subgroup._global_styles = cls._global_styles

                    elif isinstance(command, Command):
                        command._global_styles = cls._global_styles
                        group.subcommands[command.command_name] = command

                return group
            
        return wrap

    @classmethod
    def command(
        cls,
        styling: CLIStyle | None = None,
        shortnames: dict[str, str] | None = None,
        display_help_on_error: bool = True,
        error_exit_code: int = 1,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command):
            if cls._entrypoint.source:
                cls._entrypoint.command(
                    styling=styling,
                    shortnames=shortnames,
                )(command)

            return create_command(
                command,
                styling=styling,
                shortnames=shortnames,
                display_help_on_error=display_help_on_error,
                error_exit_code=error_exit_code,
            )

        return wrap

    @classmethod
    async def _dup_stdout(self, loop: asyncio.AbstractEventLoop):
        stdout_fileno = await loop.run_in_executor(None, sys.stdout.fileno)

        stdout_dup = await loop.run_in_executor(
            None,
            os.dup,
            stdout_fileno,
        )

        return await loop.run_in_executor(
            None, functools.partial(os.fdopen, stdout_dup, mode=sys.stdout.mode)
        )

    @classmethod
    async def _print_group_help_message(
        cls,
        subcommands: list[str],
    ):
        loop = asyncio.get_event_loop()

        help_message_lines: str = await cls._entrypoint.help_message.to_lines(
            subcommands=subcommands,
            global_styles=cls._global_styles,
        )

        stdout_dup = await cls._dup_stdout(loop)

        transport, protocol = await loop.connect_write_pipe(
            lambda: CLIProtocol(), 
            stdout_dup,
        )

        try:
            if has_uvloop:
                transport.close = patch_transport_close(transport, loop)

        except Exception:
            pass

        writer = asyncio.StreamWriter(
            transport,
            protocol,
            None,
            loop,
        )

        writer.write(help_message_lines.encode())
        await writer.drain()

    @classmethod
    async def _exit(
        cls,
        errors: list[str],
        exit_code: int | None,
    ):
        loop = asyncio.get_event_loop()
        if len(errors) > 0 and exit_code is not None:
            await loop.run_in_executor(
                None,
                sys.exit,
                exit_code,
            )

        await loop.run_in_executor(
            None,
            sys.exit,
            0,
        )
