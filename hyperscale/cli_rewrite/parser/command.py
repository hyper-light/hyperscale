from __future__ import annotations

import asyncio
import textwrap
import sys

from typing import Generic, TypeVar, Literal, Any, Callable
from .inspect_wrapped import inspect_wrapped, assemble_exanded_args
from .keyword_arg import KeywordArg, is_required_missing_keyword_arg
from .positional_arg import PositionalArg

T = TypeVar('T', bound=dict)
K = TypeVar('K')


ArgType = Literal['positional', 'keyword']


def create_command(
    command_call: Callable[..., Any],
    shortnames: dict[str, str] | None = None,
    
):
    (
        positional_args_map, 
        keyword_args_map, 
        help_message,
    ) = inspect_wrapped(
        command_call,
        shortnames=shortnames,
        indentation=3,
    )


    return Command(
        command_call.__name__,
        command_call,
        help_message,
        positional_args=positional_args_map,
        keyword_args_map=keyword_args_map,
    )


class Command(Generic[T]):

    def __init__(
        self,
        command: str,
        callable: T,
        help_message: str,
        positional_args: dict[str, PositionalArg] | None = None,
        keyword_args_map: dict[str, KeywordArg] | None = None,
    ):
        
        if positional_args is None:
            positional_args = {}

        if keyword_args_map is None:
            keyword_args_map = {}

        self.command_name = command
        self._command_call: T = callable

        self.help_message = help_message

        self.positional_args = positional_args
        self.keyword_args_map = keyword_args_map
        self.keyword_args_count = len(keyword_args_map)
        self.positional_args_count = len(positional_args)

    @property
    def source(self):
        if self._command_call:
            return self._command_call.__module__

    async def run(self, args: list[str]) -> tuple[Any | None, list[str]]:
        (
            positional_args, 
            keyword_args,
            errors
        ) = self._find_args(args)

        if positional_args is None and keyword_args is None:
            loop = asyncio.get_event_loop()

            await loop.run_in_executor(
                None,
                sys.stdout.write,
                textwrap.indent(f'{self.help_message}\n\n', '\t')
            )

            return (
                None,
                errors
            )

        elif len(errors) > 0:
            help_message = '\n'.join([
                errors[0],
                self.help_message,
            ])

            loop = asyncio.get_event_loop()

            await loop.run_in_executor(
                None,
                sys.stdout.write,
                textwrap.indent(f'\n{help_message}\n\n', '\t')
            )

            return (
                None, 
                errors,
            )
        
        result = await self._command_call(*positional_args, **keyword_args)

        return (
            result,
            errors,
        )

    def _find_args(self, args: list[str]):

        (
            positional_args,
            keyword_args,
            errors,
        ) = self._assembled_positional_and_keyword_args(args)

        if keyword_args.get('help'):
            return (
                None,
                None,
                None,
                None,
                errors,
            )

        if len(positional_args) < self.positional_args_count:
            errors.extend([
                f'{self.positional_args[arg_name].name} argument is config.name not in keyword_args and flag == config.full_flagrequired' for arg_name in self.positional_args if arg_name not in positional_args
            ])

        missing_required_keyword_errors = [
            f'{config.name} option is required'
            for flag, config in self.keyword_args_map.items() 
            if is_required_missing_keyword_arg(
                flag,
                config,
                keyword_args,
            )
        ]

        if len(missing_required_keyword_errors) > 0:
            errors.extend(missing_required_keyword_errors)

        keyword_args.update({
            config.name: config.parse(
                config.default
            ) 
            for flag, config in self.keyword_args_map.items() 
            if is_required_missing_keyword_arg(
                flag,
                config,
                keyword_args,
            )
        })

        return (
            positional_args, 
            keyword_args, 
            errors,
        )
    
    def _assembled_positional_and_keyword_args(
        self,
        args: list[str],
    ):
        positional_args: list[Any] = []
        keyword_args: dict[str, Any] = {}
        consumed_idxs: list[int] = []
        positional_idx = 0

        errors: list[str] = []

        cli_args = assemble_exanded_args(args)

        for idx, arg in enumerate(cli_args):

            error: str | None = None
        
            if (
                keyword_arg := self.keyword_args_map.get(arg)
            ):
                consumed_idxs.append(idx)
                (
                    value,
                    error,
                    consumed_idxs
                ) = self._consume_keyword_value(
                    idx,
                    cli_args,
                    keyword_arg,
                    consumed_idxs,
                )

                keyword_args[keyword_arg.name] = value

            elif (
                positional_arg := self.positional_args.get(positional_idx)
            ) and idx not in consumed_idxs:
                positional_args, error = self._consume_positional_value(
                    arg,
                    positional_arg,
                    positional_args,
                )

                positional_idx += 1

                consumed_idxs.append(idx)

            elif idx not in consumed_idxs and error is None:
                error = f'{arg} is not a recognized argument or command'

            if error:
                errors.append(error)

        return (
            positional_args,
            keyword_args,
            errors
        )
    
    def _consume_positional_value(
        self,
        arg: str,
        positional_arg: PositionalArg,
        positional_args: list[Any]
    ):
        
        if not isinstance(arg, positional_arg.value_type):
            return (
                positional_args,
                f'{arg} is not a valid value {positional_arg.data_type} for argument {positional_arg.name}',
                
            )
        
        positional_args.append(
            positional_arg.parse(arg)
        )
        
        return (
            positional_args,
            None,
        )

    def _consume_keyword_value(
        self,
        current_idx: int,
        args: list[str], 
        keyword_arg: KeywordArg,
        consumed_idxs: list[int],
    ):

        if keyword_arg.arg_type == 'flag':
            return (
                True, 
                None, 
                consumed_idxs,
            )

        value_index = 0

        for arg_idx, arg in enumerate(args):

            if arg.startswith('-'):
                value_index += 1

            elif isinstance(keyword_arg.parse(arg), Exception):
                value_index += 1

            elif arg_idx <= current_idx:
                value_index += 1

            else:
                break

        value_missing = value_index >= len(args)
        value: Any | None = None

        if value_missing and keyword_arg.required:
            return (
                None,
                f'No valid value found for required option {keyword_arg.full_flag}',
                consumed_idxs,
            ) 

        elif value_missing and keyword_arg.required is False:
            value = keyword_arg.default

        else:
            value = args[value_index]
            consumed_idxs.append(value_index)
        
        return (
            keyword_arg.parse(value),
            None,
            consumed_idxs,
        )
