import asyncio
import random
import uuid
import time
from pydantic import BaseModel, StrictStr, Field
from hyperscale.distributed.server.events import (
    LamportRunner,
)


initially_granted_proc = "A"
procs = {"A", "B", "C", "D"}


class Message(BaseModel):
    data: StrictStr
    name: StrictStr
    id: StrictStr = Field(default_factory=lambda: str(uuid.uuid4()))

    def __hash__(self):
        return hash(self.id)


async def run():
    t1 = LamportRunner("A")
    t2 = LamportRunner("B")
    t3 = LamportRunner("C")
    t4 = LamportRunner("D")

    t1.subscribe(t2)
    t2.subscribe(t1)

    t1.subscribe(t3)
    t3.subscribe(t1)

    t1.subscribe(t4)
    t4.subscribe(t1)

    t2.subscribe(t3)
    t3.subscribe(t2)
    
    t2.subscribe(t4)
    t4.subscribe(t2)

    t3.subscribe(t4)
    t4.subscribe(t3)

    await t1.update()

    t1.run()
    t2.run()
    t3.run()
    t4.run()

    print('GEN')
    start = time .monotonic()

    nodes = [t1, t2, t3, t4]

    while (time.monotonic() - start) < 60:

        await asyncio.sleep(1)

        idxs = [
            random.randrange(0, 4) for _ in range(random.randrange(1, 4))
        ]

        print(idxs)

        await asyncio.gather(*[
            nodes[idx].update() for idx in idxs
        ])


    print('Node 1:', t1.clock.time)
    print('Node 2:', t2.clock.time)
    print('Node 3:', t3.clock.time)
    print('Node 4:', t4.clock.time)
    await t1.stop()
    await t2.stop()
    await t3.stop()
    await t4.stop()

asyncio.run(run())