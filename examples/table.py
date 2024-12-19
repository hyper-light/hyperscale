import asyncio

from hyperscale.terminal.components.table import (
    Table,
    TableConfig,
)


async def test():
    data = [
        {
            "one": 1,
            "two": 2,
            "three": 3,
            "four": "bleh",
        },
        {
            "one": 2,
            "two": 1,
            "three": 3,
        },
    ]

    table = Table(
        TableConfig(
            headers={
                "one": {
                    "field_type": "integer",
                },
                "two": {
                    "field_type": "integer",
                },
                "three": {
                    "precision": ".2f",
                    "field_type": "integer",
                },
                "four": {"field_type": "string"},
            }
        )
    )

    await table.fit(100, 10)

    await table.update(data)

    print(await table.get_next_frame())


asyncio.run(test())
