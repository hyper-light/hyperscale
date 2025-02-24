from enum import Enum


class RequestType(Enum):
    CUSTOM = 'CUSTOM'
    HTTP = "HTTP"
    HTTP2 = "HTTP2"
    HTTP3 = "HTTP3"
    WEBSOCKET = "WEBSOCKET"
    GRAPHQL = "GRAPHQL"
    GRAPHQL_HTTP2 = "GRAPHQL_HTTP2"
    GRPC = "GRPC"
    PLAYWRIGHT = "PLAYWRIGHT"
    SMTP = "SMTP"
    TCP = "TCP"
    UDP = "UDP"