import textwrap

from typing import Callable, Any

from .keyword_arg import KeywordArg
from .positional_arg import PositionalArg


def args_to_string(
    args: list[PositionalArg] | list[KeywordArg],
    header_name: str,
    param_descriptors: dict[str, str],
    indentation: int | None = None,
):
    if indentation is None:
        indentation = 0

    tabs = ' ' * indentation
    join_char = f'\n{tabs}'
    
    arg_string = join_char.join([
        arg.to_help_string(
            descriptor=param_descriptors.get(arg.name)
        ) for arg in args
    ])

    header_indentation = max(indentation - 1, 0)
    header_indentation_tabs = f' ' * header_indentation
    header_join_char = f'\n{header_indentation_tabs}'

    return f'{header_join_char}{header_name}:{join_char}{arg_string}'


def is_arg_descriptor(line: str):
    stripped_line = line.strip()
    return stripped_line.startswith('@param') or stripped_line.startswith(':param')


def create_description(
    help_message: str,
    indentation: int | None = None
):
    if indentation is None:
        indentation = 0
    
    tabs = ' ' * indentation
    join_char = f'\n{tabs}'

    help_message_lines = join_char.join([
        line
        for line in textwrap.dedent(
            help_message
        ).split('\n') 
        if not is_arg_descriptor(line) and len(line.strip()) > 0
    ])
    
    return f'{join_char}{help_message_lines}'


def create_subcommands_description(
    subcommands_or_subgroups: list[str],
    indentation: int | None = None
):
    
    if indentation is None:
        indentation = 0

    tabs = ' ' * indentation
    join_char = f'\n{tabs}'

    subcommands_string = join_char.join(subcommands_or_subgroups)

    header_indentation = max(indentation - 1, 0)
    header_indentation_tabs = f' ' * header_indentation
    header_join_char = f'\n{header_indentation_tabs}'
    
    return f'{header_join_char}commands:{join_char}{subcommands_string}'


def map_doc_string_param_descriptors(doc_string: str):
    param_lines = [
        line.strip()
        for line in doc_string.split('\n') 
        if is_arg_descriptor(line)
    ]

    param_descriptors: dict[str, str] = {}

    for line in param_lines:
        if line.startswith('@param'):
            cleaned_line = line.strip('@param').strip()
            name, descriptor = cleaned_line.split(' ', maxsplit=1)

            param_descriptors[name] = descriptor

        elif line.startswith(':param'):
            cleaned_line = line.strip(':param').strip()
            _, name, descriptor = cleaned_line.split(' ', maxsplit=2)

            param_descriptors[name] = descriptor

    return param_descriptors


def create_help_string(
    command_name: str,
    help_string: str,
    keyword_args_map: dict[str, KeywordArg] | None = None,
    indentation: int | None = None
):
    
    if indentation is None:
        indentation = 0

    if help_string is None:
        help_string = "No description found..."


    description_string = create_description(
        help_string,
        indentation=indentation,
    )

    keyword_args_string: str | None = None
    options_string: str | None = None

    param_descriptors = map_doc_string_param_descriptors(help_string)

    if keyword_args_map is None:
        keyword_args_map = {}

    if len(keyword_args_map) > 0:

        sorted_arg_keys = sorted(
            keyword_args_map.keys()
        )

        keyword_args = [keyword_args_map[arg_name] for arg_name in sorted_arg_keys if arg_name == keyword_args_map[arg_name].full_flag]
        keyword_args_string = args_to_string(
            keyword_args,
            'options',
            param_descriptors,
            indentation=indentation,
        )

        options_string = ','.join([
            arg.to_flag() for arg in keyword_args
        ])

    help_message_header = f'\n{command_name} [{options_string}]'
    help_message_lines = [
        help_message_header,
        description_string,
        keyword_args_string,
    ]

    return '\n'.join([
        line for line in help_message_lines if line 
    ])