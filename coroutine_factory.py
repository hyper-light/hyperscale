import asyncio
import functools

from hyperscale.terminal.components import Link, Spinner


async def coro(message: str):
    return message


def create(message: str):
    return functools.partial(coro, message)


async def main():
    spinner = Spinner()

    # await spinner.spin(text="Hello world!", color="hot_pink_5", mode="extended")
    # await asyncio.sleep(10)

    # await spinner.ok(color="aquamarine_2", text="Done!", mode="extended")

    # await spinner.stop()

    # text = Text("Hello world!", color="blue")

    # message = await text.style(color="blue_violet", mode="extended")

    link = Link("This is a link", "https://www.google.com/")
    styled_link = await link.style(color="cadet_blue", mode="extended")

    print(styled_link)


asyncio.run(main())
