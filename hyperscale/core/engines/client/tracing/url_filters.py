from urllib.parse import urlparse


def default_params_strip_filter(url: str) -> str:
    return urlparse(url)._replace(
        query=None
    ).geturl()

