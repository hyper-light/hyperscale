from hyperscale.core_rewrite.engines.client.http.models.http import HTTPResponse


class GraphQLResponse(HTTPResponse):

    class Config:
        arbitrary_types_allowed=True
