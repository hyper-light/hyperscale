from __future__ import annotations

from typing import Dict, Literal

from .attribute_type import AttributeType as AttributeType

AttributeName = Literal[
    "bold", "dark", "italic", "underline", "blink", "reverse", "concealed"
]


class Attribute:
    names: Dict[
        AttributeName,
        int,
    ] = {attr.name.lower(): attr.value for attr in AttributeType}

    types: Dict[
        AttributeType,
        int,
    ] = {attr: attr.value for attr in AttributeType}

    def __iter__(self):
        for name in self.names:
            yield name

    def __contains__(self, attribute: AttributeName):
        return attribute in self.names

    @classmethod
    def by_name(cls, attribute: AttributeName):
        return cls.names[attribute]

    @classmethod
    def by_type(cls, attribute: AttributeType):
        return cls.types[attribute]
