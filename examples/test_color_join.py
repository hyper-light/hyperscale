import asyncio
from collections import OrderedDict

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.styling import stylize


async def style():
    print(hasattr(OrderedDict(), "__len__"))
    styles = await asyncio.gather(
        *[
            stylize("a", color="aquamarine_3", mode=TerminalMode.EXTENDED),
            stylize("b", color="hot_pink_4", mode=TerminalMode.EXTENDED),
            stylize("c", color="white"),
        ]
    )

    print("".join(styles))


asyncio.run(style())
