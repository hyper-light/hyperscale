import asyncio
from hyperscale.ui import HyperscaleInterface


async def run():
    ui = HyperscaleInterface()

    ui.run()
    await asyncio.sleep(10)

    await ui.stop()


asyncio.run(run())
