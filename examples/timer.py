import asyncio
import time
import sys
from hyperscale.ui.components.timer import (
    Timer,
    TimerConfig,
)

async def run():
    timer = Timer(
        'test',
        TimerConfig(
            color='aquamarine_2',
            terminal_mode='extended',
        )
    )

    print("\033[?25l")

    await timer.fit(max_width=12)
    
    loop = asyncio.get_event_loop()
    start = time.monotonic()
    elapsed = 0

    # word = " hello "

    await timer.update()

    while elapsed < 3650:
        frame, _ = await timer.get_next_frame()
        loop.run_in_executor(None, sys.stdout.write, '\033[3J\033[H\n' + frame.pop())
        
        await asyncio.sleep(1/30)
        elapsed = time.monotonic() - start

    print("\033[?25h")


asyncio.run(run())