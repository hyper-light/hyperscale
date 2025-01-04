from typing import Any, Dict

import msgspec

from .log_level import LogLevel


class Entry(msgspec.Struct, kw_only=True):
    message: str | None = None
    tags: set[str] = msgspec.field(
        default_factory=set,
    )
    level: LogLevel

    def to_template(
        self,
        template: str,
        context: Dict[str, Any] | None = None,
    ):
        kwargs: Dict[
            str,
            int | str | bool | float | LogLevel | list | dict | set | Any,
        ] = {field: getattr(self, field) for field in self.__struct_fields__}

        kwargs["level"] = kwargs["level"].value

        if context:
            kwargs.update(context)

        return template.format(**kwargs)
