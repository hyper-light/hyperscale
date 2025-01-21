from hyperscale.commands.cli.arg_types import KeywordArg


from .cli_style import CLIStyle
from .description_help_message import DescriptionHelpMessage
from .help_message import HelpMessage
from .options_help_message import OptionsHelpMessage
from .title_help_message import TitleHelpMessage


def create_help_string(
    command_name: str,
    help_string: str,
    indentation: int | None = None,
    keyword_args_map: dict[str, KeywordArg] | None = None,
    styling: CLIStyle | None = None,
):
    if indentation is None:
        indentation = 0

    if help_string is None:
        help_string = "No description found..."

    sorted_arg_keys = sorted(keyword_args_map.keys())
    options = [
        keyword_args_map[arg_name]
        for arg_name in sorted_arg_keys
        if arg_name == keyword_args_map[arg_name].full_flag
    ]

    return HelpMessage(
        title=TitleHelpMessage(
            command=command_name,
            options=options,
            styling=styling,
        ),
        description=DescriptionHelpMessage(
            help_string=help_string,
            indentation=indentation,
            styling=styling,
        ),
        options=OptionsHelpMessage(
            options=options,
            help_string=help_string,
            indentation=indentation,
            styling=styling,
        ),
        styling=styling,
    )
