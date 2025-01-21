from typing import Any, Dict, Iterator, List

from pydantic import BaseModel

DataValue = str | bytes | Iterator | Dict[str, Any] | List[str] | BaseModel
OptimizedData = bytes | List[bytes]
