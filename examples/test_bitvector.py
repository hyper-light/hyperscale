import sys
import asyncio
import zstandard
from hyperscale.distributed.server.events import LamportClock
from hyperscale.distributed.models import BitVector


async def test():
    clock = LamportClock()
    vec = BitVector(size=128)

    comp = zstandard.ZstdCompressor()

    vec[0:64] = b'bc0.0.0.0:8080<ack_ack<ack'

    message = b'bc<0.0.0.0:8080<ack_ack<ack'
    size = len(message)
    packed = int.from_bytes(message)
    data = comp.compress(message)
    print(data, sys.getsizeof(data))
    packed_bytes = packed.to_bytes(size)
    print(packed_bytes, sys.getsizeof(packed_bytes))



asyncio.run(test())