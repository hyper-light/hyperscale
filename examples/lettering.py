import asyncio
from hyperscale.ui.components.header import Header, HeaderConfig


async def run():
    header = Header(
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
        )
    )

    await header.fit(100, 10)
    print(await header.get_next_frame())


asyncio.run(run())
