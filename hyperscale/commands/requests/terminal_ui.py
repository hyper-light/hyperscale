
from hyperscale.ui.components.header import Header, HeaderConfig
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.terminal import Terminal, action
from typing import Any, Callable


@action()
async def update_redirects(redirects: int):
    return f'Redirects: {redirects}'


@action()
async def update_elapsed(elapsed: float):
    return f'Elapsed: {elapsed:.2f}s'

@action()
async def update_text(message: str):
    return message


@action()
async def update_status(status: int | str):
    return str(status)


@action()
async def update_params(
    response_params: list[tuple[str, str]],
    request_params: dict[str, str],
):
    
    request_params.update({
        param_name: param_value for param_name, param_value in response_params
    })

    params = [
        {
            "param": param_name,
            "value": param_value,
        } for param_name, param_value in request_params.items()
    ]

    return params

@action()
async def update_headers(headers: dict[str, Any] | None):

    if headers is None:
        return {}

    return [
        {
            "header": header_name,
            "value": header_value,
        } for header_name, header_value in headers.items()
    ]


@action()
async def update_cookies(cookies: tuple[str, str]):
    return [
        {
            "cookie": cookie_name,
            "value": cookie_value,
        } for cookie_name, cookie_value in cookies
    ]

def colorize_status(status: str):

    try:
        status = int(status)

    except Exception:
        return status

    if isinstance(status, int) is False:
        return "white"
    
    if status >= 200 and status < 300:
        return "aquamarine_2"
    
    return "hot_pink_3"

def colorize_udp_or_tcp(status: str):
    
    if status == "OK":
        return "aquamarine_2"
    
    return "hot_pink_3"


def colorize_smtp(status: str):

    try:
        status = int(status)

    except Exception:
        return status
    
    if status == 250:
        return "aquamarine_2"
    
    return "hot_pink_3"

def map_status_to_error(status: int):

    if status >= 300 and status < 400:
        return "Redirect"

    errors = {
        400: "Bad request",
        401: "Unauthorized",
        402: "Payment required",
        403: "Forbidden",
        404: "Not found",
        405: "Method not allowed",
        406: "Not acceptable",
        407: "Proxy auth required",
        408: "Timeout",
        409: "Conflict",
        410: "Gone",
        411: "Length required",
        412: "Precondition failed",
        413: "Request too large",
        414: "URI too long",
        415: "Unsupported media type",
        416: "Range not satisfiable",
        417: "Expectation failed",
        418: "I'm a teapot",
        419: "Page expired",
        420: "Too many requests",
        421: "Misdirect",
        422: "Unprocessable entity",
        423: "Locked",
        424: "Failed Dependency",
        425: "Too early",
        426: "Upgrade required",
        428: "Precondition required",
        429: "Too many requests",
        431: "Headers field too large",
        451: "Unavailable for legal reasons",
        500: "Internal server error",
        501: "Not implemented",
        502: "Bad gateway",
        503: "Service unavailable",
        504: "Gateway timeout",
        505: "HTTP version not supported",
        506: "Variant also negotiates",
        507: "Insufficient storage",
        508: "Loop detected",
        510: "Not extended",
        511: "Network auth required"
    }

    return errors.get(
        status,
        "Encountered unknown err.",
    )


def create_ping_ui(
    url: str,
    method: str,
    override_status_colorizer: Callable[[int], str] | None = None
):
    
    status_colorizer = colorize_status
    if override_status_colorizer:
        status_colorizer = override_status_colorizer
    
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

    redirects_display = Section(
        SectionConfig(
            width="x-small",
            height="xx-small",
            max_height=3,
            left_border="|",
            top_border="-",
            bottom_border="-",
            vertical_alignment="center",
        ),
        components=[
            Text(
                "redirects_display",
                TextConfig(
                    text=f'Redirects: ...',
                ),
                subscriptions=['update_redirects']
            )
        ],
    )

    elapsed_display = Section(
        SectionConfig(
            width="x-small",
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
            height="xx-small",
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

    method_display = Section(
        SectionConfig(
            height="xx-small",
            width="x-small",
            right_border="|",
            top_border="-",
            bottom_border="-",
            max_height=3,
        ),
        components=[
            Text(
                "method_display",
                TextConfig(
                    text=method.upper(),
                ),
            )
        ]
    )

    status_display = Section(
        SectionConfig(
            height="xx-small",
            width="x-small",
            right_border="|",
            top_border="-",
            bottom_border="-",
            max_height=3,
        ),
        components=[
            Text(
                "status_display",
                TextConfig(
                    text="Status: ...",
                    color=status_colorizer,
                    terminal_mode='extended'
                ),
                subscriptions=["update_status"]
            )
        ]
    )


    params_table = Section(
        SectionConfig(
            width="small",
            height="medium",
            left_border="|",
            top_border="-",
            bottom_border="-",
            left_padding=2,
            right_padding=2,
            horizontal_alignment="center",
        ),
        components=[
            Table(
                "params_table",
                TableConfig(
                    headers={
                        "param": {
                            "default": "...",
                            "fixed": True,
                        },
                        "value": {
                            "default": "...",
                            "fixed": True
                        },
                    },
                    minimum_column_width=8,
                    border_color="aquamarine_2",
                    terminal_mode="extended",
                    table_format="simple",
                    cell_alignment="LEFT",
                ),
                subscriptions=["update_params"],
            )
        ],
    )

    headers_table = Section(
        SectionConfig(
            width="small",
            height="medium",
            left_border="|",
            top_border="-",
            bottom_border="-",
            left_padding=2,
            right_padding=2,
            horizontal_alignment="center",
        ),
        components=[
            Table(
                "headers_table",
                TableConfig(
                    headers={
                        "header": {
                            "default": "...",
                            "fixed": True,
                        },
                        "value": {
                            "default": "...",
                            "fixed": True
                        },
                    },
                    minimum_column_width=8,
                    border_color="aquamarine_2",
                    terminal_mode="extended",
                    table_format="simple",
                    cell_alignment="LEFT",
                ),
                subscriptions=["update_headers"],
            )
        ],
    )

    cookies_table = Section(
        SectionConfig(
            width="small",
            height="medium",
            left_border="|",
            top_border="-",
            right_border="|",
            bottom_border="-",
            left_padding=2,
            right_padding=2,
            horizontal_alignment="center",
        ),
        components=[
            Table(
                "cookies_table",
                TableConfig(
                    headers={
                        "cookie": {
                            "default": "...",
                            "fixed": True,
                        },
                        "value": {
                            "default": "...",
                            "fixed": True
                        },
                    },
                    minimum_column_width=8,
                    border_color="aquamarine_2",
                    terminal_mode="extended",
                    table_format="simple",
                    cell_alignment="LEFT",
                ),
                subscriptions=["update_cookies"],
            )
        ],
    )

    terminal = Terminal(
        [
            header,
            redirects_display,
            elapsed_display,
            ping_text_display,
            url_display,
            method_display,
            status_display,
            params_table,
            headers_table,
            cookies_table,
        ]
    )

    return terminal