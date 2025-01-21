import asyncio
import time
from hyperscale.ui.components.animated_status_bar import (
    AnimatedStatusBar,
    AnimatedStatusBarConfig,
)


async def run():
    bar = AnimatedStatusBar(
        "test",
        AnimatedStatusBarConfig(
            default_status="ready",
            animations={
                "ready": {
                    "animations": ["stripe", "highlight", "color"],
                    "direction": "bounce",
                    "primary_color": "black",
                    "primary_highlight": "aquamarine_2",
                    "secondary_color": "black",
                },
                "success": {"primary_highlight": "sky_blue_2"},
                "failed": {"primary_highlight": "light_red"},
            },
            horizontal_padding=2,
            terminal_mode="extended",
        ),
    )

    print("\033[?25l")

    await bar.fit(max_width=10)

    start = time.monotonic()
    elapsed = 0

    # word = " hello "

    while elapsed < 60:
        frame, _ = await bar.get_next_frame()

        await asyncio.sleep(0.06)
        print("\033[3J\033[H\n" + frame.pop())
        elapsed = time.monotonic() - start

    print("\033[?25h")


asyncio.run(run())
