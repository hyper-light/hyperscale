import asyncio

from hyperscale.ui.components.terminal import (
    Alignment,
    Component,
)
from hyperscale.ui.components.table import (
    Table,
    TableConfig,
)


async def test():
    data = [
        {
            "one": 1,
            "two": 2,
            "three": 3,
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
            },
            cell_alignment="RIGHT",
            header_color_map={
                "one": "aquamarine_2",
            },
            data_color_map={
                "two": lambda value: "hot_pink_3" if "2" == value else None
            },
            table_color="aquamarine_2",
            terminal_mode="extended",
            table_format="simple",
        )
    )

    component = Component(
        "table_test",
        table,
        Alignment(
            horizontal="left",
        ),
        horizontal_padding=8,
    )

    await component.fit(46, 10)

    await component.component.update(data)

    print("\n".join(await component.render()))


asyncio.run(test())
