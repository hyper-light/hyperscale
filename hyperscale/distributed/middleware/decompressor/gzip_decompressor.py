from gzip import decompress
from typing import Callable, Dict, Tuple, Union

from pydantic import BaseModel

from hyperscale.distributed.middleware.base import Middleware, MiddlewareType
from hyperscale.distributed.models.http import Request, Response


class GZipDecompressor(Middleware):
    def __init__(
        self,
        compression_level: int = 9,
        serializers: Dict[
            str, Callable[[Union[Response, BaseModel, str, None]], Union[str, None]]
        ] = {},
    ) -> None:
        super().__init__(
            self.__class__.__name__, middleware_type=MiddlewareType.UNIDIRECTIONAL_AFTER
        )

        self.compression_level = compression_level
        self.serializers = serializers

    async def __run__(
        self,
        request: Request,
        response: Union[Response, BaseModel, str, None],
        status: int,
    ) -> Tuple[Tuple[Response, int], bool]:
        try:
            if response is None:
                return (
                    request,
                    Response(request.path, request.method, data=response),
                    status,
                ), True

            elif isinstance(response, str):
                decompressed_data = decompress(response.encode())

                return (
                    request,
                    Response(
                        request.path,
                        request.method,
                        headers={"content-type": "text/plain"},
                        data=decompressed_data.decode(),
                    ),
                    status,
                ), True

            else:
                headers = response.headers
                content_encoding = headers.get(
                    "content-encoding", headers.get("x-compression-encoding")
                )

                if content_encoding == "gzip":
                    serialized = self.serializers[request.path](response)
                    decompressed_data = decompress(serialized)

                    headers.pop(
                        "content-encoding", headers.pop("x-compression-encoding", None)
                    )

                    return (
                        request,
                        Response(
                            request.path,
                            request.method,
                            headers=headers,
                            data=decompressed_data.decode(),
                        ),
                        status,
                    ), True

                return (response, status), True

        except KeyError:
            return (
                request,
                Response(
                    request.path,
                    request.method,
                    data=f"No serializer for {request.path} found.",
                ),
                500,
            ), False

        except Exception as e:
            return (
                request,
                Response(request.path, request.method, data=str(e)),
                500,
            ), False
