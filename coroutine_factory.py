import asyncio
import functools

from hyperscale.terminal.spinner import AsyncSpinner
from hyperscale.terminal.text import Text


async def coro(message: str):
    return message


def create(message: str):
    return functools.partial(coro, message)


async def main():
    spinner = AsyncSpinner()

    await spinner.spin(text="Hello world!", color=54, mode="extended")
    await asyncio.sleep(10)

    await spinner.fail(color="red")

    await spinner.stop()

    text = Text("Hello world!", color="blue")

    message = await text.style()

    print(message)


asyncio.run(main())
