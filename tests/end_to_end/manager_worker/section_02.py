import asyncio

from tests.end_to_end.manager_worker.section_runner import run_section


async def run() -> None:
    await run_section(2)


if __name__ == "__main__":
    asyncio.run(run())
