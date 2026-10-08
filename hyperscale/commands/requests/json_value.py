"""``JSONValue`` -- any value ``json.loads`` produces: what the ping
commands parse from their JSON-encoded ``--headers``, ``--data`` and
GraphQL ``variables`` options."""

type JSONValue = str | int | float | bool | None | list[JSONValue] | dict[str, JSONValue]
