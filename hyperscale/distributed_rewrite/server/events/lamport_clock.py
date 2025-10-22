import asyncio



class LamportClock:

    def __init__(self):
        self.time = 0
        self._lock = asyncio.Lock()

    async def increment(self):
        async with self._lock:
            self.time += 1
            return self.time
        
    async def update(self, time: int):
        async with self._lock:
            self.time = max(time, self.time + 1)
            return self.time
    
    async def ack(self, time: int):
        async with self._lock:
            self.time = max(time, self.time)