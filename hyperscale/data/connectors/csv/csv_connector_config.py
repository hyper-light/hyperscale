from typing import List, Optional

from pydantic import BaseModel, StrictStr

from hyperscale.data.connectors.common.connector_type import ConnectorType


class CSVConnectorConfig(BaseModel):
    filepath: StrictStr
    file_mode: StrictStr = "r"
    reporter_type: ConnectorType = ConnectorType.CSV
    headers: Optional[List[StrictStr]]
