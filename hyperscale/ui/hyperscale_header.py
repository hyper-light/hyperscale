from hyperscale.ui.components.header import Header, HeaderConfig
from hyperscale.ui.config.mode import TerminalDisplayMode


def create_hyperscale_header(terminal_mode: TerminalDisplayMode) -> Header:
    """The Hyperscale header every hyperscale terminal UI opens with: the
    word in the header font, with the letters' spacing tuned for it."""
    return Header(
        "header",
        HeaderConfig(
            header_text="hyperscale",
            formatters={
                "y": [lambda letter, _: "\n".join([" " + line for line in letter.split("\n")])],
                "l": [
                    lambda letter, _: "\n".join(
                        [
                            line[:-1] if line_index == 2 else line
                            for line_index, line in enumerate(letter.split("\n"))
                        ]
                    )
                ],
                "e": [
                    lambda letter, letter_index: "\n".join(
                        [
                            line[1:] if line_index < 2 else line
                            for line_index, line in enumerate(letter.split("\n"))
                        ]
                    )
                    if letter_index == 9
                    else letter
                ],
            },
            color="aquamarine_2",
            attributes=["bold"],
            terminal_mode=terminal_mode,
        ),
    )
