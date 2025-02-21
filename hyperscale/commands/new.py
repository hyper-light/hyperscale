import asyncio
import inspect
import pathlib
import sys
import textwrap

try:

    import uvloop
    uvloop.install()

except Exception:
    pass

from hyperscale.ui.components.header import Header, HeaderConfig
from hyperscale.ui.components.terminal import Section, SectionConfig, Terminal
from hyperscale.ui.components.text import Text, TextConfig

from .cli import CLI
from .workflow import test



async def create_new_file_ui(
    path: str,
    failed_overwrite_check: bool,
):
    header = Section(
        SectionConfig(
            left_padding=4,
            height="smallest",
            width="full",
            max_height=3,
        ),
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

    text_component = Text(
        "new_test_display",
        TextConfig(
            horizontal_alignment="left",
            text=f"New test created at {path}",
        ),
    )

    if failed_overwrite_check:
        text_component = Text(
            "new_test_display",
            TextConfig(
                horizontal_alignment="left",
                text=f"File already exists at {path}! Please run again with the --overwrite/-o flag.",
            ),
        )

    test_text_display = Section(
        SectionConfig(
            left_padding=3,
            width="medium",
            height="xx-small",
            left_border=" ",
            top_border=" ",
            right_border=" ",
            bottom_border=" ",
            max_height=3,
            vertical_alignment="center",
        ),
        components=[text_component],
    )

    terminal = Terminal(
        [
            header,
            test_text_display,
        ]
    )

    terminal_text = await terminal.render_once()

    return f"\033[2J\033[H\n{terminal_text}\n"


async def create_test(
    loop: asyncio.AbstractEventLoop,
    path: str,
):
    test_file = await loop.run_in_executor(
        None,
        open,
        path,
        "w",
    )

    try:
        await loop.run_in_executor(
            None,
            test_file.write,
            textwrap.dedent(inspect.getsource(test)),
        )

    except Exception:
        pass

    await loop.run_in_executor(None, test_file.close)


@CLI.command()
async def new(
    path: str,
    overwrite: bool = False,
):
    """
    Create a new test at the provided path

    @param path The path to create the test file at
    @param overwrite If specified, if a file exists at the provided path it will be overwritten
    """
    loop = asyncio.get_event_loop()

    test_path = await loop.run_in_executor(
        None,
        pathlib.Path,
        path,
    )

    absolute_path = await loop.run_in_executor(None, test_path.absolute)

    resolved_path = await loop.run_in_executor(None, absolute_path.resolve)

    filepath_exists = await loop.run_in_executor(None, resolved_path.exists)

    if filepath_exists and overwrite is True:
        await create_test(
            loop,
            str(resolved_path),
        )

        await loop.run_in_executor(
            None, sys.stdout.write, await create_new_file_ui(path, False)
        )

    elif filepath_exists is False:
        await create_test(
            loop,
            str(resolved_path),
        )

        await loop.run_in_executor(
            None, sys.stdout.write, await create_new_file_ui(path, False)
        )

    else:
        await loop.run_in_executor(
            None, sys.stdout.write, await create_new_file_ui(path, True)
        )
