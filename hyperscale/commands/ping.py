import json
from typing import Literal, Any
from hyperscale.core.engines.client.time_parser import TimeParser
from .requests import (
    make_graphql_request,
    make_graphqlh2_request,
    make_http_request,
    make_http2_request,
    make_http3_request,
    make_udp_request,
    make_websocket_request,
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


def load_data(data: str | None):

    if data is None:
        return data

    try:
        return load_data(data)
    
    except Exception:
        return data


@CLI.command()
async def ping(
    url: str,
    method: AssertSet[
        Literal[
            "get",
            "post",
            "put",
            "patch",
            "delete",
            "head",
            "options",
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
            "http",
            "http2",
            "http3",
            "udp",
            "graphql",
            "graphqlh2",
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
    '''
    
    timeout_seconds = TimeParser(timeout).time
    
    match client.data:
        case "graphql":
            return await make_graphql_request(
                url,
                method=method.data,       
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
                method=method.data,       
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
                method=method.data,       
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
                method=method.data,       
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
                method=method.data,       
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
        
        case "udp":
            return await make_udp_request(
                url,
                method=method.data,
                data=data,
                options=options,
                timeout=timeout,
                output_file=filepath,
                wait=wait,
                quiet=quiet,
            )
        
        case "websocket":
            return await make_websocket_request(
                url,
                method=method.data,       
                cookies=[
                    cookie.strip().split(
                        "=",
                    ) for cookie in cookies.split(";")
                ] if cookies else None,
                params=json.loads(params),
                headers=json.loads(headers),
                data=load_data(data),
                redirects=redirects,
                timeout=timeout,
                output_file=filepath,
                wait=wait,
                quiet=quiet,
            )
            