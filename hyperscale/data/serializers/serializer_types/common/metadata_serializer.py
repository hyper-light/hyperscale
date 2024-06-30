from typing import Dict, List, Union

from hyperscale.core.engines.types.common.metadata import Metadata


class MetadataSerializer:
    def __init__(self) -> None:
        self.user: Union[str, None] = None
        self.tags: List[Dict[str, str]] = []

    def serialize_metadata(
        self, metadata: Metadata
    ) -> Dict[str, Union[str, List[str]]]:
        return {"user": metadata.user, "tags": metadata.tags}
