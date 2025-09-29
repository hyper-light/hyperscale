from enum import Enum


class RequestType(Enum):
    CUSTOM = 'CUSTOM'
    FTP = "FTP"
    GRAPHQL = "GRAPHQL"
    GRAPHQL_HTTP2 = "GRAPHQL_HTTP2"
    GRPC = "GRPC"
    HTTP = "HTTP"
    HTTP2 = "HTTP2"
    HTTP3 = "HTTP3"
    PLAYWRIGHT = "PLAYWRIGHT"
    SCP = "SCP"
    SFTP = "SFTP"
    SMTP = "SMTP"
    TCP = "TCP"
    UDP = "UDP"
    WEBSOCKET = "WEBSOCKET"