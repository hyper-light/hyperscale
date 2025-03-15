import asyncio
import json
from ipaddress import IPv4Address, IPv6Address
from typing import Literal, Any
from hyperscale.core.engines.client.time_parser import TimeParser
from .requests import (
    make_graphql_request,
    make_graphqlh2_request,
    make_http_request,
    make_http2_request,
    make_http3_request,
    make_smtp_request,
    make_tcp_request,
    make_udp_request,
    make_websocket_request,
    lookup_url,
)
from .cli import (
    CLI,
    AssertSet
)

def get_default_output_filepath():
    return None

def get_default_sender():
    return 'ping@hyperscale.org'

def get_default_subject():
    return 'Hyperscale Ping Test'

def get_default_options():
    return json.dumps({})

def get_default_cookies():
    return None

def get_default_params():
    return json.dumps({})

def get_default_headers():
    return json.dumps({})

def get_default_email():
    return "This is a ping test from Hyperscale's SMTP client!"

def get_default_auth():
    return ''

def get_default_recipients():
    return ''


def get_default_data():
    return None


def load_options(options: str | None):
    try:
        return json.loads(options)
    
    except Exception:
        return {}

def load_data(data: str | None):

    if data is None:
        return data

    try:
        return load_data(data)
    
    except Exception:
        return data
    
def is_missing_http_prefix(url: str):

    address = url.split(':', maxsplit=1)
    host: str | None = None
    
    if len(address) == 2:
        host, _ = address

    elif len(address) == 1:
        host = address[0]

    if url.startswith("http://") or url.startswith('https://'):
        return False
    
    elif isinstance(
        host,
        IPv4Address
    ) or isinstance(
        host,
        IPv6Address
    ):
        return False
    
    return True

def is_missing_websocket_prefix(url: str):

    address = url.split(':', maxsplit=1)
    host: str | None = None
    
    if len(address) == 2:
        host, _ = address

    elif len(address) == 1:
        host = address[0]

    if url.startswith("ws://"):
        return False
    
    elif isinstance(
        host,
        IPv4Address
    ) or isinstance(
        host,
        IPv6Address
    ):
        return False
    
    return True

def is_missing_smtp_prefix(url: str):

    address = url.split(':', maxsplit=1)
    host: str | None = None
    
    if len(address) == 2:
        host, _ = address

    elif len(address) == 1:
        host = address[0]

    if url.startswith("smtp"):
        return False
    
    elif isinstance(
        host,
        IPv4Address
    ) or isinstance(
        host,
        IPv6Address
    ):
        return False
    
    return True

@CLI.group()
async def ping():
    '''
    Run one-off requests or perform IP lookups using Hyperscale's clients
    '''


@ping.command(
    shortnames={
        'headers': 'H',
    }
)
async def graphql(
    url: str,
    query: str,
    method: AssertSet[
        Literal[
            "query",
            "mutation",
        ]
    ] = "query",
    headers: str = get_default_headers,
    redirects: int = 3,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off GraphQL request

    @param url The url to use for the request
    @param query A string of or string-encoded object/blob query to submit for the request
    @param method The method or request type to use
    @param headers A string, JSON compatible string/any map of headers (i.e. '{"<NAME>": <VALUE>}')
    @param redirects The maximum number of redirects to follow
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if is_missing_http_prefix(url):
        url = f'http://{url}'

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    return await make_graphql_request(
        url,
        method=method_name,
        headers=json.loads(headers),
        data=load_data(query),
        redirects=redirects,
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,

    )


@ping.command(
    shortnames={
        'headers': 'H',
    }
)
async def graphqlh2(
    url: str,
    query: str,
    method: AssertSet[
        Literal[
            "query",
            "mutation",
        ]
    ] = "query",
    headers: str = get_default_headers,
    redirects: int = 3,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off GraphQL HTTP2 request

    @param url The url to use for the request
    @param query A string of or string-encoded object/blob query to submit for the request
    @param method The method or request type to use
    @param headers A string, JSON compatible string/any map of headers (i.e. '{"<NAME>": <VALUE>}')
    @param redirects The maximum number of redirects to follow
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if is_missing_http_prefix(url):
        url = f'https://{url}'

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    return await make_graphqlh2_request(
        url,
        method=method_name,
        headers=json.loads(headers),
        data=load_data(query),
        redirects=redirects,
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,

    )


@ping.command(
    shortnames={
        'headers': 'H',
    }
)
async def http(
    url: str,
    method: AssertSet[
        Literal[
            "GET",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "HEAD",
            "OPTIONS",
        ]
    ] = "get",
    cookies: str = get_default_cookies,
    params: str = get_default_params,
    headers: str = get_default_headers,
    data: str = get_default_data,
    redirects: int = 3,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off HTTP request

    @param url The url to use for the request
    @param method The method or request type to use
    @param cookies A string, semi-colon delimited list of <NAME>=<VALUE> cookies
    @param params A string, JSON compatible string/string map of query parameters (i.e. '{"<NAME>": "<VALUE>"}')
    @param headers A string, JSON compatible string/any map of headers (i.e. '{"<NAME>": <VALUE>}')
    @param data A string of or string-encoded object/blob data to submit for the request
    @param redirects The maximum number of redirects to follow
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if is_missing_http_prefix(url):
        url = f'http://{url}'

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    try:
          
        return await make_http_request(
            url,
            method=method_name,       
            cookies=[
                cookie.strip().split(
                    "=",
                ) for cookie in cookies.split(";")
            ] if cookies else None,
            params=json.loads(params),
            headers=json.loads(headers),
            data=load_data(data),
            redirects=redirects,
            timeout=timeout_seconds,
            output_file=filepath,
            wait=wait,
            quiet=quiet,

        )
    
    except Exception:
        import traceback
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, print, traceback.format_exc())

@ping.command(
    shortnames={
        'headers': 'H',
    }
)
async def http2(
    url: str,
    method: AssertSet[
        Literal[
            "GET",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "HEAD",
            "OPTIONS",
        ]
    ] = "get",
    cookies: str = get_default_cookies,
    params: str = get_default_params,
    headers: str = get_default_headers,
    data: str = get_default_data,
    redirects: int = 3,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off HTTP2 request

    @param url The url to use for the request
    @param method The method or request type to use
    @param cookies A string, semi-colon delimited list of <NAME>=<VALUE> cookies
    @param params A string, JSON compatible string/string map of query parameters (i.e. '{"<NAME>": "<VALUE>"}')
    @param headers A string, JSON compatible string/any map of headers (i.e. '{"<NAME>": <VALUE>}')
    @param data A string of or string-encoded object/blob data to submit for the request
    @param client The client to use
    @param options A string, JSON compatible string/any map of arbitrary config options
    @param redirects The maximum number of redirects to follow
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if is_missing_http_prefix(url):
        url = f'https://{url}'

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    return await make_http2_request(
        url,
        method=method_name,       
        cookies=[
            cookie.strip().split(
                "=",
            ) for cookie in cookies.split(";")
        ] if cookies else None,
        params=json.loads(params),
        headers=json.loads(headers),
        data=load_data(data),
        redirects=redirects,
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,
    )


@ping.command(
    shortnames={
        'headers': 'H',
    }
)
async def http3(
    url: str,
    method: AssertSet[
        Literal[
            "GET",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "HEAD",
            "OPTIONS",
        ]
    ] = "get",
    cookies: str = get_default_cookies,
    params: str = get_default_params,
    headers: str = get_default_headers,
    data: str = get_default_data,
    redirects: int = 3,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off HTTP3 request

    @param url The url to use for the request
    @param method The method or request type to use
    @param cookies A string, semi-colon delimited list of <NAME>=<VALUE> cookies
    @param params A string, JSON compatible string/string map of query parameters (i.e. '{"<NAME>": "<VALUE>"}')
    @param headers A string, JSON compatible string/any map of headers (i.e. '{"<NAME>": <VALUE>}')
    @param data A string of or string-encoded object/blob data to submit for the request
    @param redirects The maximum number of redirects to follow
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if is_missing_http_prefix(url):
        url = f'https://{url}'

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    return await make_http3_request(
        url,
        method=method_name,       
        cookies=[
            cookie.strip().split(
                "=",
            ) for cookie in cookies.split(";")
        ] if cookies else None,
        params=json.loads(params),
        headers=json.loads(headers),
        data=load_data(data),
        redirects=redirects,
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,
    )
        

@ping.command(     
    shortnames={
        'subject': 'S',
    }
)
async def smtp(
    url: str,
    auth: str = get_default_auth,
    email: str = get_default_email,
    sender: str = get_default_sender,
    subject: str = get_default_subject,
    recipients: str = get_default_recipients,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off SMTP request

    @param url The url to use for the email server
    @param auth A colon-delimited tuple "<USERNAME>:<PASSWORD>" to use when authenticating
    @param email The string email body to send
    @param sender The sender of the email
    @param subject The title/subject of the email
    @param recipients A comma-delimited list of recipient email addresses (i.e. "<TEST@TEST.EX>,<TEST@TEST_1.EX>")
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if is_missing_smtp_prefix(url):
        url = f'smtp.{url}'

    if lookup:
        return await lookup_url(
            url,
            as_smtp=True,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time
    
    return await make_smtp_request(
        url,
        auth=auth,
        email=email,
        sender=sender,
        subject=subject,
        recipients=recipients,
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,

    )
        

@ping.command()
async def tcp(
    url: str,
    method: AssertSet[
        Literal[
            "send",
            "receive",
            "bidirectional",
        ]
    ] = "send",
    data: str = get_default_data,
    options: str = get_default_options,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off TCP request

    @param url The url to use for the request
    @param method The method or request type to use
    @param data A string of or string-encoded object/blob data to submit for the request
    @param options A string, JSON compatible string/any map of arbitrary config options
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    return await make_tcp_request(
        url,
        method=method_name,
        data=data,
        options=load_options(options),
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,
    )
        

@ping.command()
async def udp(
    url: str,
    method: AssertSet[
        Literal[
            "send",
            "receive",
            "bidirectional",
        ]
    ] = "send",
    data: str = get_default_data,
    options: str = get_default_options,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off UDP request

    @param url The url to use for the request
    @param method The method or request type to use
    @param data A string of or string-encoded object/blob data to submit for the request
    @param options A string, JSON compatible string/any map of arbitrary config options
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    return await make_udp_request(
        url,
        method=method_name,
        data=data,
        options=load_options(options),
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,
    )
        

@ping.command(
    shortnames={
        'headers': 'H',
    }
)
async def websocket(
    url: str,
    method: AssertSet[
        Literal[
            "GET",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "HEAD",
            "OPTIONS",
            "send",
            "receive",
            "bidirectional",
            "query",
            "mutation",
        ]
    ] = "get",
    cookies: str = get_default_cookies,
    params: str = get_default_params,
    headers: str = get_default_headers,
    data: str = get_default_data,
    redirects: int = 3,
    timeout: str = "1m",
    filepath: str = get_default_output_filepath,
    lookup: bool = False,
    wait: bool = False,
    quiet: bool = False,
):
    '''
    Run a one-off request using one of the supported client types

    @param url The url to use for the request
    @param method The method or request type to use
    @param cookies A string, semi-colon delimited list of <NAME>=<VALUE> cookies
    @param params A string, JSON compatible string/string map of query parameters (i.e. '{"<NAME>": "<VALUE>"}')
    @param headers A string, JSON compatible string/any map of headers (i.e. '{"<NAME>": <VALUE>}')
    @param data A string of or string-encoded object/blob data to submit for the request
    @param redirects The maximum number of redirects to follow
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if is_missing_websocket_prefix(url):
        url = f'ws://{url}'


    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    return await make_websocket_request(
        url,
        method=method_name,       
        cookies=[
            cookie.strip().split(
                "=",
            ) for cookie in cookies.split(";")
        ] if cookies else None,
        params=json.loads(params),
        headers=json.loads(headers),
        data=load_data(data),
        redirects=redirects,
        timeout=timeout_seconds,
        output_file=filepath,
        wait=wait,
        quiet=quiet,
    )
            