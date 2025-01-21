from __future__ import annotations

import inspect

from typing import Any, Callable

from .arg_types import (
    Context,
    KeywordArg,
    PositionalArg,
)
from .help_message import create_help_string, CLIStyle


def is_context_arg(
    arg: str,
    idx: int,
    keyword_args_map: dict[str, KeywordArg],
    positional_args_map: dict[str, PositionalArg],
):
    keyword_args_map: dict[str, KeywordArg] = {}
    positional_args_map: dict[str, PositionalArg] = {}

    if (keyword_arg := keyword_args_map.get(arg)) and keyword_arg.is_context_arg:
        return True

    elif (
        positional_arg := positional_args_map.get(idx)
    ) and positional_arg.is_context_arg:
        return True

    return False


def assemble_exanded_args(args: list[str]):
    cli_args: list[str] = []

    for arg in args:
        if arg.startswith("--"):
            cli_args.append(arg)

        elif arg.startswith("-"):
            expanded_args = [f"-{short_arg}" for short_arg in list(arg.strip("-"))]

            cli_args.extend(expanded_args)

        else:
            cli_args.append(arg)

    return cli_args


def inspect_wrapped(
    command_call: Callable[..., Any],
    styling: CLIStyle | None = None,
    shortnames: dict[str, str] | None = None,
    indentation: int | None = None,
):
    if indentation is None:
        indentation = 0

    call_args = inspect.signature(command_call)

    positional_args_map: dict[int, PositionalArg] = {}
    keyword_args_map: dict[str, KeywordArg] = {}

    position_index: int = 0

    for arg_name, arg_attrs in call_args.parameters.items():
        if (
            arg_attrs.default == inspect._empty
            and arg_attrs.annotation == inspect._empty
        ):
            raise Exception(
                f"Err. - cannot use unannotated arg {arg_name} for command signature"
            )

        elif arg_attrs.default == inspect._empty:
            positional_args_map[position_index] = PositionalArg(
                arg_name,
                position_index,
                arg_attrs.annotation,
                is_context_arg=Context == arg_attrs.annotation,
            )

            position_index += 1

        else:
            arg_type: KeywordArgType = "keyword"
            arg_default = arg_attrs.default
            if isinstance(arg_attrs.default, bool) or arg_attrs.annotation == bool:
                arg_type = "flag"

            if arg_type == "flag" and arg_attrs.default is None:
                arg_default = False

            required = True if arg_attrs.default is None else False

            keyword_arg = KeywordArg(
                arg_name,
                arg_attrs.annotation,
                short_name=shortnames.get(arg_name),
                required=required,
                default=arg_default,
                arg_type=arg_type,
                is_context_arg=Context == arg_attrs.annotation,
            )

            keyword_args_map[keyword_arg.full_flag] = keyword_arg
            keyword_args_map[keyword_arg.short_flag] = keyword_arg

    help_arg = KeywordArg(
        "help",
        bool,
        required=False,
        description="Display the help message and exit.",
        arg_type="flag",
    )

    keyword_args_map.update(
        {
            help_arg.full_flag: help_arg,
            help_arg.short_flag: help_arg,
        }
    )

    help_message = create_help_string(
        command_call.__name__,
        command_call.__doc__,
        keyword_args_map=keyword_args_map,
        styling=styling,
        indentation=indentation,
    )

    return (
        positional_args_map,
        keyword_args_map,
        help_message,
    )
