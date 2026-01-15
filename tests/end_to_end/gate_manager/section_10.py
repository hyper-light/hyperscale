import asyncio

from tests.end_to_end.gate_manager.section_runner import run_section


async def run() -> None:
    await run_section(10)


if __name__ == "__main__":
    asyncio.run(run())
