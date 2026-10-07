from typing import Dict, Iterator, List

from pydantic import BaseModel

from hyperscale.core.testing.models.base.base_types import HTTPEncodableValue

DataValue = str | bytes | Iterator | Dict[str, HTTPEncodableValue] | List[str] | BaseModel
OptimizedData = bytes | List[bytes]
