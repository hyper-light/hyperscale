import asyncio
import functools

from hyperscale.terminal.components import Spinner


async def coro(message: str):
    return message


def create(message: str):
    return functools.partial(coro, message)


async def main():
    spinner = Spinner()

    await spinner.spin(color="hot_pink_5", mode="extended")

    await asyncio.sleep(10)

    await spinner.ok(color="aquamarine_2", text="Done!", mode="extended")

    await spinner.stop()


asyncio.run(main())
