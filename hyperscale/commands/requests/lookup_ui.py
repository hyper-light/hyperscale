from ipaddress import IPv4Address, ip_address
from socket import AddressFamily, SocketKind
from typing import Any

from hyperscale.ui.components.header import Header, HeaderConfig
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.terminal import Terminal, action


@action()
async def update_elapsed(elapsed: float):
    return f'Elapsed: {elapsed:.3f}s'

@action()
async def update_text(message: str):
    return message

@action()
async def update_socket_info(socket_info: list[
    tuple[
        tuple[str, int], 
        tuple[AddressFamily, SocketKind, int, str, tuple[str, int]]
    ]
]):
    
    socket_rows = []

    for info in socket_info:
        _, socket_config = info
        family, socket_kind, _, _, address = socket_config
        host, port = address

        if isinstance(ip_address(host), IPv4Address):
            ip_type = 'ipv4'

        else:
            ip_type = 'ipv6'

        socket_rows.append({
            "ip": host,
            "port": port,
            "family": family.name,
            "socket type": socket_kind.name,
            "ip type": ip_type,
        })

    return socket_rows

@action()
async def update_socket_info_smtp(socket_info: list[
    tuple[AddressFamily, SocketKind, int, str, tuple[str, int]]
]):
    
    socket_rows = []

    for info in socket_info:
        family, socket_kind, _, _, address = info
        host, port = address

        if isinstance(ip_address(host), IPv4Address):
            ip_type = 'ipv4'

        else:
            ip_type = 'ipv6'

        socket_rows.append({
            "ip": host,
            "port": port,
            "family": family.name,
            "socket type": socket_kind.name,
            "ip type": ip_type,
        })

    return socket_rows

def colorize_status(status: str):

    try:
        status = int(status)

    except Exception:
        pass

    if isinstance(status, int) is False:
        return "white"
    
    if status >= 200 and status < 300:
        return "aquamarine_2"
    
    return "hot_pink_3"


def create_lookup_ui(
    url: str,
):
    
    header = Section(
        SectionConfig(
            height="xx-small", 
            width="full",
        ),
        components=[
            Header(
                "header",
                HeaderConfig(
                    header_text="hyperscale",
                    formatters={
                        "y": [
                            lambda letter, _: "\n".join(
                                [" " + line for line in letter.split("\n")]
                            )
                        ],
                        "l": [
                            lambda letter, _: "\n".join(
                                [
                                    line[:-1] if idx == 2 else line
                                    for idx, line in enumerate(letter.split("\n"))
                                ]
                            )
                        ],
                        "e": [
                            lambda letter, idx: "\n".join(
                                [
                                    line[1:] if idx < 2 else line
                                    for idx, line in enumerate(letter.split("\n"))
                                ]
                            )
                            if idx == 9
                            else letter
                        ],
                    },
                    color="aquamarine_2",
                    attributes=["bold"],
                    terminal_mode="extended",
                ),
            ),
        ],
    )

    elapsed_display = Section(
        SectionConfig(
            width="medium",
            height="xx-small",
            max_height=3,
            left_border="|",
            top_border="-",
            bottom_border="-",
            vertical_alignment="center",
        ),
        components=[
            Text(
                "elapsed_display",
                TextConfig(
                    text=f'Elapsed: ...',
                ),
                subscriptions=['update_elapsed']
            )
        ],
    )

    ping_text_display = Section(
        SectionConfig(
            width="medium",
            height="xx-small",
            max_height=3,
            left_border="|",
            top_border="-",
            bottom_border="-",
            right_border="|",
            vertical_alignment="center",
        ),
        components=[
            Text(
                "ping_text_display",
                TextConfig(
                    text="Ready...",
                    color='hot_pink_3',
                    terminal_mode='extended',
                ),
                subscriptions=["update_text"]
            )
        ],
    )

    url_display = Section(
        SectionConfig(
            height="full",
            width="medium",
            max_height=3,
            left_border="|",
            right_border="|",
            top_border="-",
            bottom_border="-",
        ),
        components=[
            Text(
                "url_display",
                TextConfig(
                    text=url,
                ),
            )
        ]
    )

    socket_info_table = Section(
        SectionConfig(
            width="full",
            height="medium",
            left_border="|",
            top_border="-",
            bottom_border="-",
            right_border="|",
            left_padding=2,
            right_padding=2,
            horizontal_alignment="center",
        ),
        components=[
            Table(
                "params_table",
                TableConfig(
                    headers={
                        "ip": {
                            "default": "...",
                            "fixed": True,
                        },
                        "port": {
                            "default": "...",
                            "fixed": True
                        },
                        "family": {
                            "default": "..."
                        },
                        "socket type": {
                            "default": "..."
                        },
                        "ip type": {
                            "default": "..."
                        }
                    },
                    minimum_column_width=8,
                    border_color="aquamarine_2",
                    terminal_mode="extended",
                    table_format="simple",
                ),
                subscriptions=["update_socket_info"],
            )
        ],
    )

 

    terminal = Terminal(
        [
            header,
            elapsed_display,
            ping_text_display,
            url_display,
            socket_info_table,
        ]
    )

    return terminal