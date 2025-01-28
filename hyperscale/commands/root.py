import asyncio
import sys
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.terminal import Terminal
from hyperscale.ui.components.header import Header, HeaderConfig
from .cli import (
    CLI,
    CLIStyle,
)
from .run import run


async def create_header():
    header = Section(
        SectionConfig(height="smallest", width="large", max_height=3),
        components=[
            Header(
                "header",
                HeaderConfig(
                    header_text="hyperscale",
                    formatters={
                        "y": [
                            lambda letter, _: "\n".join(
                                [" " + line for line in letter.split("\n")]
                            )
                        ],
                        "l": [
                            lambda letter, _: "\n".join(
                                [
                                    line[:-1] if idx == 2 else line
                                    for idx, line in enumerate(letter.split("\n"))
                                ]
                            )
                        ],
                        "e": [
                            lambda letter, idx: "\n".join(
                                [
                                    line[1:] if idx < 2 else line
                                    for idx, line in enumerate(letter.split("\n"))
                                ]
                            )
                            if idx == 9
                            else letter
                        ],
                    },
                    color="aquamarine_2",
                    attributes=["bold"],
                    terminal_mode="extended",
                ),
            ),
        ],
    )

    terminal = Terminal(
        [
            header,
        ]
    )

    return await terminal.render_once()


@CLI.root(
    run,
    global_styles=CLIStyle(
        header=create_header,
        flag_description_color="white",
        error_color="hot_pink_3",
        error_attributes=["italic"],
        flag_color="aquamarine_2",
        text_color="hot_pink_3",
        subcommand_color="hot_pink_3",
        indentation=5,
        terminal_mode="extended",
    ),
)
async def hyperscale():
    """
    The Hyperscale next-generation performance testing framework
    """


def run():
    try:
        asyncio.run(CLI.run(args=sys.argv[1:]))

    except KeyboardInterrupt:
        pass
