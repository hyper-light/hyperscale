from typing import Any, Dict, List, Union

from hyperscale.core.engines.types.common.timeouts import Timeouts
from hyperscale.core.engines.types.common.types import RequestTypes
from hyperscale.core.engines.types.websocket.action import WebsocketAction
from hyperscale.core.engines.types.websocket.client import MercuryWebsocketClient
from hyperscale.core.engines.types.websocket.result import WebsocketResult
from hyperscale.data.serializers.serializer_types.common.base_serializer import (
    BaseSerializer,
)


class WebsocketSerializer(BaseSerializer):
    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self, action: WebsocketAction
    ) -> Dict[str, Union[str, List[str]]]:
        serialized_action = super().action_to_serializable(action)
        return {
            **serialized_action,
            "type": RequestTypes.HTTP,
            "url": {
                "full": action.url.full,
                "ip_addr": action.url.ip_addr,
                "socket_config": action.url.socket_config,
                "has_ip_addr": action.url.has_ip_addr,
            },
            "method": action.method,
            "headers": action._headers,
            "header_items": action._header_items,
            "encoded_headers": action.encoded_headers,
            "data": action.data,
            "encoded_data": action.encoded_data,
            "is_stream": action.is_stream,
            "is_setup": action.is_setup,
            "action_args": action.action_args,
        }

    def deserialize_action(self, action: Dict[str, Any]) -> WebsocketAction:
        url_config = action.get("url", {})
        metadata = action.get("metadata", {})

        websocket_action = WebsocketAction(
            name=action.get("name"),
            url=url_config.get("full"),
            headers=action.get("headers"),
            data=action.get("data"),
            user=metadata.get("user"),
            tags=metadata.get("tags", []),
        )

        websocket_action.url.ip_addr = url_config.get("ip_addr")
        websocket_action.url.socket_config = url_config.get("socket_config")
        websocket_action.url.has_ip_addr = url_config.get("has_ip_addr")

        websocket_action.setup()

        return websocket_action

    def deserialize_client_config(
        self, client_config: Dict[str, Any]
    ) -> MercuryWebsocketClient:
        return MercuryWebsocketClient(
            concurrency=client_config.get("concurrency"),
            timeouts=Timeouts(**client_config.get("timeouts", {})),
            reset_connections=client_config.get("reset_sessions"),
        )

    def result_to_serializable(self, result: WebsocketResult) -> Dict[str, Any]:
        serialized_result = super().result_to_serializable(result)

        encoded_headers = {
            str(k.decode()): str(v.decode()) for k, v in result.headers.items()
        }

        body: Union[str, None] = None
        if result.body:
            body = str(result.body.decode())

        return {
            **serialized_result,
            "url": result.url,
            "method": result.method,
            "path": result.path,
            "params": result.params,
            "query": result.query,
            "type": result.type,
            "headers": encoded_headers,
            "body": body,
            "tags": result.tags,
            "user": result.user,
            "error": str(result.error),
            "status": result.status,
            "reason": result.reason,
        }

    def deserialize_result(self, result: Dict[str, Any]) -> WebsocketResult:
        deserialized_result = WebsocketResult(
            WebsocketResult(
                name=result.get("name"),
                url=result.get("url"),
                method=result.get("method"),
                headers=result.get("headers"),
                data=result.get("data"),
                user=result.get("user"),
                tags=result.get("tags", []),
            ),
            error=Exception(result.get("error")),
        )

        body = result.get("body")
        if isinstance(body, str):
            body = body.encode()

        deserialized_result.body = body
        deserialized_result.status = result.get("status")
        deserialized_result.reason = result.get("reason")
        deserialized_result.params = result.get("params")
        deserialized_result.query = result.get("query")
        deserialized_result.wait_start = result.get("wait_start")
        deserialized_result.start = result.get("start")
        deserialized_result.connect_end = result.get("connect_end")
        deserialized_result.write_end = result.get("write_end")
        deserialized_result.complete = result.get("complete")
        deserialized_result.checks = result.get("checks")

        deserialized_result.type = RequestTypes.WEBSOCKET

        return deserialized_result
