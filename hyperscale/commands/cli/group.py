from __future__ import annotations

import asyncio
import functools
import os
import sys

from typing import Generic, TypeVar, Any, Callable
try:
    import uvloop as uvloop
    has_uvloop = True

except Exception:
    has_uvloop = False

from .arg_types import (
    Context,
    KeywordArg,
    is_required_missing_keyword_arg,
    is_defaultable,
    is_env_defaultable,
    is_unsupported_keyword_arg,
    PositionalArg,
)
from .cli_protocol import CLIProtocol, patch_transport_close
from .command import Command, create_command
from .help_message import HelpMessage, CLIStyle
from .inspect_wrapped import inspect_wrapped, assemble_exanded_args, is_context_arg


T = TypeVar("T", bound=dict)
K = TypeVar("K")


def create_group(
    command_call: Callable[..., Any],
    styling: CLIStyle | None = None,
    shortnames: dict[str, str] | None = None,
):
    indentation = 0
    if styling:
        indentation = styling.indentation

    (
        positional_args_map,
        keyword_args_map,
        help_message,
    ) = inspect_wrapped(
        command_call,
        styling=styling,
        shortnames=shortnames,
        indentation=indentation,
    )

    return Group(
        command_call.__name__,
        command_call,
        help_message,
        positional_args=positional_args_map,
        keyword_args_map=keyword_args_map,
    )


class Group(Generic[T]):
    def __init__(
        self,
        command: str,
        callable: T,
        help_message: HelpMessage,
        positional_args: dict[str, PositionalArg] | None = None,
        keyword_args_map: dict[str, KeywordArg] | None = None,
    ):
        if positional_args is None:
            positional_args = {}

        if keyword_args_map is None:
            keyword_args_map = {}

        keyword_args_map.update(
            {
                keyword_arg.short_name: keyword_arg
                for keyword_arg in keyword_args_map.values()
            }
        )

        self.group_name = command
        self._command_call: T = callable
        self.help_message = help_message

        self.subgroups: dict[str, Group[Any]] = {}
        self.subcommands: dict[str, Command[Any]] = {}
        self._global_styles: CLIStyle | None = None

        self.positional_args = positional_args
        self.keyword_args_map = keyword_args_map
        self.keyword_args_count = len(keyword_args_map)
        self.positional_args_count = len(positional_args)
        self._loop = asyncio.get_event_loop()

    def update_command(
        self,
        command: str,
        callable: T,
        help_message: HelpMessage,
        global_styles: CLIStyle | None = None,
        positional_args: dict[str, PositionalArg] | None = None,
        keyword_args_map: dict[str, KeywordArg] | None = None,
    ):
        self._global_styles = global_styles

        if positional_args is None:
            positional_args = {}

        if keyword_args_map is None:
            keyword_args_map = {}

        keyword_args_map.update(
            {
                keyword_arg.short_name: keyword_arg
                for keyword_arg in keyword_args_map.values()
            }
        )

        self.group_name = command
        self._command_call: T = callable
        self.help_message = help_message

        self.positional_args = positional_args
        self.keyword_args_map = keyword_args_map
        self.keyword_args_count = len(keyword_args_map)
        self.positional_args_count = len(positional_args)

    async def run(
        self,
        args: list[str],
        context: Context[str, Any],
    ) -> tuple[Any | None, list[str]]:
        subcommands: list[str] | None = None
        if len(self.subgroups) > 0 or len(self.subcommands) > 0:
            subcommands = list(self.subgroups.keys())
            subcommands.extend(self.subcommands.keys())

        if len(args) < 1:
            await self._print_group_help_message(subcommands)

            return (None, [])

        (
            positional_args,
            keyword_args,
            subcommand,
            subcommand_args,
            errors,
        ) = await self._find_args(args, context)

        if positional_args is None and keyword_args is None:
            await self._print_group_help_message(subcommands)

            return (None, errors)

        elif len(errors) > 0:
            await self._print_group_help_message(
                error=errors[0],
                subcommands=subcommands,
            )

            return (None, errors)

        result: Any | None = None
        if len(positional_args) > 0 or len(keyword_args) > 0:
            result = await self._command_call(*positional_args, **keyword_args)

        if subcommand:
            subcommand._global_styles = self._global_styles
            result, errors = await subcommand.run(subcommand_args, context)

        return (
            result,
            errors,
        )

    @property
    def source(self):
        if self._command_call:
            return self._command_call.__module__

    def group(
        self,
        styling: CLIStyle | None = None,
        shortnames: dict[str, str] | None = None,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command):
            group = create_group(command, styling=styling, shortnames=shortnames)

            self.subgroups[group.group_name] = group

            return group

        return wrap

    def command(
        self,
        styling: CLIStyle | None = None,
        shortnames: dict[str, str] | None = None,
    ):
        if shortnames is None:
            shortnames = {}

        def wrap(command_call):
            cmd = create_command(
                command_call,
                styling=styling,
                shortnames=shortnames,
            )

            self.subcommands[cmd.command_name] = cmd

            return command_call

        return wrap
    
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


    async def _print_group_help_message(
        self,
        subcommands: list[str],
        error: str | None = None
    ):
        loop = asyncio.get_event_loop()

        help_message_lines = await self.help_message.to_lines(
            error=error,
            subcommands=subcommands,
            global_styles=self._global_styles,
        )

        stdout_dup = await self._dup_stdout(loop)

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


        await loop.run_in_executor(
            None,
            writer.write,
            help_message_lines.encode(),
        )

    async def _find_args(
        self,
        args: list[str],
        context: Context[str, Any],
    ):
        (
            positional_args,
            keyword_args,
            subcommand,
            offset,
            errors,
        ) = await self._assembled_positional_and_keyword_args(
            args,
            context,
        )

        if keyword_args.get("help"):
            return (
                None,
                None,
                None,
                None,
                errors,
            )

        if len(positional_args) < self.positional_args_count:
            errors.extend(
                [
                    f"{self.positional_args[arg_name].name} argument is required"
                    for arg_name in self.positional_args
                    if arg_name not in positional_args
                ]
            )

        missing_required_keyword_errors = [
            f"{config.full_flag} option is required"
            for flag, config in self.keyword_args_map.items()
            if is_required_missing_keyword_arg(
                flag,
                config,
                keyword_args,
            )
            and Context not in config.value_type
        ]

        keyword_args.update(
            {
                config.name: await config.to_default()
                if Context not in config.value_type
                else context
                for flag, config in self.keyword_args_map.items()
                if is_defaultable(
                    flag,
                    config,
                    keyword_args,
                )
            }
        )

        keyword_args.update(
            {
                config.name: await config.parse()
                if Context not in config.value_type
                else context
                for flag, config in self.keyword_args_map.items()
                if is_env_defaultable(
                    flag,
                    config,
                    keyword_args,
                )
            }
        )

        missing_required_keyword_errors = [
            f"{config.full_flag} option is required"
            for flag, config in self.keyword_args_map.items()
            if is_required_missing_keyword_arg(
                flag,
                config,
                keyword_args,
            )
            and Context not in config.value_type
        ]

        if len(missing_required_keyword_errors) > 0:
            errors.extend(missing_required_keyword_errors)

        return (
            positional_args,
            keyword_args,
            subcommand,
            args[offset:],
            errors,
        )

    async def _assembled_positional_and_keyword_args(
        self,
        args: list[str],
        context: Context[str, Any],
    ):
        positional_args: list[Any] = []
        keyword_args: dict[str, Any] = {}
        consumed_idxs: set[int] = set()
        positional_idx = 0

        errors: list[str] = []
        subcommand: Command | Group | None = None

        cli_args = assemble_exanded_args(args)

        cli_args = [
            arg
            for idx, arg in enumerate(cli_args)
            if is_context_arg(
                arg,
                idx,
                self.keyword_args_map,
                self.positional_args,
            )
            is False
        ]

        keyword_args_map = {
            arg_name: config
            for arg_name, config in self.keyword_args_map.items()
            if Context not in config.value_type
        }

        positional_args_map: dict[str, PositionalArg] = {}

        position_arg_idx = 0
        for config in self.positional_args.values():
            if Context not in config.value_type:
                positional_args_map[position_arg_idx] = config
                position_arg_idx += 1

        for idx, arg in enumerate(cli_args):
            error: str | None = None

            if keyword_arg := keyword_args_map.get(arg):
                consumed_idxs.add(idx)
                (value, error, consumed_idxs) = await self._consume_keyword_value(
                    idx,
                    cli_args[idx + 1 :],
                    keyword_arg,
                    consumed_idxs,
                )

                keyword_args[keyword_arg.name] = value
                consumed_idxs.add(idx)

            elif (command := self.subcommands.get(arg)) or (
                command := self.subgroups.get(arg)
            ):
                subcommand = command
                consumed_idxs.add(idx)

                break

            elif is_unsupported_keyword_arg(arg, keyword_args_map):
                errors.append(Exception(f"unsupported option {arg}"))

            elif (
                positional_arg := self.positional_args.get(positional_idx)
            ) and idx not in consumed_idxs:
                positional_args, error = await self._consume_positional_value(
                    arg,
                    positional_arg,
                    positional_args,
                )

                positional_idx += 1

                consumed_idxs.add(idx)

            if error:
                errors.append(error)
                return (
                    positional_args,
                    keyword_args,
                    subcommand,
                    None,
                    errors,
                )

        offset = len(consumed_idxs)

        context_indexes = [
            arg.index
            for arg in self.positional_args.values()
            if Context in arg.value_type
        ]

        for index in context_indexes:
            positional_args.insert(index, context)

        context_keyword_args = [
            arg.name
            for arg in self.keyword_args_map.values()
            if Context in arg.value_type
        ]

        for keyword_arg_name in context_keyword_args:
            keyword_args[keyword_arg_name] = context

        return (
            positional_args,
            keyword_args,
            subcommand,
            offset,
            errors,
        )

    async def _consume_positional_value(
        self,
        arg: str,
        positional_arg: PositionalArg,
        positional_args: list[Any],
    ):
        value = await positional_arg.parse(arg)

        if isinstance(value, Exception):
            return (
                positional_args,
                f"{arg} is not a valid value {positional_arg.data_type} for argument {positional_arg.name}",
            )

        positional_args.append(value)

        return (
            positional_args,
            None,
        )

    async def _consume_keyword_value(
        self,
        current_idx: int,
        args: list[str],
        keyword_arg: KeywordArg,
        consumed_idxs: set[int],
    ):
        if keyword_arg.arg_type == "flag":
            return (
                True,
                None,
                consumed_idxs,
            )

        value: Any | None = None
        value_idx = 0
        last_error: Exception | None = None

        for arg_idx, arg in enumerate(args):
            if arg.startswith("-"):
                value_idx = arg_idx + 1

            else:
                break

        args_size = len(args)
        if args_size == 0 or value_idx >= args_size:
            return (
                value,
                Exception(f'No value found for option {keyword_arg.full_flag}'),
                consumed_idxs,
            )

        result = await keyword_arg.parse(args[value_idx])
        value, last_error = self._return_value_and_error(result)

        if value is None and keyword_arg.loads_from_envar:
            result = await keyword_arg.parse()
            value, last_error = self._return_value_and_error(result)

        if value is None and isinstance(last_error, Exception):
            return (
                value,
                last_error,
                consumed_idxs,
            )

        elif value is None and keyword_arg.required is False:
            value = await keyword_arg.to_default()

        consumed_idx = current_idx + value_idx + 1

        consumed_idxs.add(consumed_idx)

        return (
            value,
            None,
            consumed_idxs,
        )

    def _return_value_and_error(self, value: Any | Exception):
        if isinstance(value, Exception):
            return None, value

        return value, None
