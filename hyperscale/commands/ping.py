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

def get_default_options():
    return json.dumps({})

def get_default_cookies():
    return None

def get_default_params():
    return json.dumps({})

def get_default_headers():
    return json.dumps({})


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


@CLI.command(
    shortnames={
        'headers': 'H'
    }
)
async def ping(
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
    client: AssertSet[
        Literal[
            "graphql",
            "graphqlh2",
            "http",
            "http2",
            "http3",
            "tcp",
            "udp",
            "websocket"
        ]
    ] = "http",
    options: str = get_default_options,
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
    @param client The client to use
    @param options A string, JSON compatible string/any map of arbitrary config options
    @param redirects The maximum number of redirects to follow
    @param timeout The request timeout
    @param filepath Output the request results to the specified filepath
    @param lookup Execute only the IP address lookup and output matches
    @param wait Don't exit once the request completes or fails
    @param quiet Mutes all terminal output
    '''

    if client.data in ['http', 'graphql'] and is_missing_http_prefix(url):
        url = f'http://{url}'

    elif client.data in ['http2', 'http3', 'graphqlh2'] and is_missing_http_prefix(url):
        url = f'https://{url}'

    if lookup:
        return await lookup_url(
            url,
            wait=wait,
            quiet=quiet,
        )
    
    timeout_seconds = TimeParser(timeout).time

    method_name = method.data.lower()
    
    match client.data:
        case "graphql":
            return await make_graphql_request(
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
        
        case "graphqlh2":
            return await make_graphqlh2_request(
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


        case "http":
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
        
        case "http2":
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
        
        case "http3":
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
        
        case "tcp":
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
        
        case "udp":
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
        
        case "websocket":
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
            