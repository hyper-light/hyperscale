from .graphql import make_graphql_request as make_graphql_request
from .graphql_http2 import make_graphqlh2_request as make_graphqlh2_request
from .http import make_http_request as make_http_request
from .http2 import make_http2_request as make_http2_request
from .http3 import make_http3_request as make_http3_request
from .lookup import lookup_url as lookup_url
from .udp import make_udp_request as make_udp_request
from .websocket import make_websocket_request as make_websocket_request