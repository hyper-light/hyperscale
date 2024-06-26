import uuid
from datetime import datetime

from hyperscale.core.engines.types.common.types import RequestTypes

try:
    from cassandra.cqlengine import columns
    from cassandra.cqlengine.models import Model

    has_connector = True

except Exception:
    columns = object
    Model = None
    has_connector = False


class CassandraTaskResultSchema:
    def __init__(self, table_name: str) -> None:
        self.results_columns = {
            "id": columns.UUID(primary_key=True, default=uuid.uuid4),
            "name": columns.Text(min_length=1, index=True),
            "source": columns.Text(),
            "data": columns.Text(),
            "error": columns.Text(),
            "wait_start": columns.Float(),
            "start": columns.Float(),
            "complete": columns.Float(),
            "user": columns.Text(),
            "tags": columns.List(columns.Map(columns.Text(), columns.Text())),
            "created_at": columns.DateTime(default=datetime.now),
        }

        self.results_table = type(f"{table_name}_task", (Model,), self.results_columns)

        self.type = RequestTypes.TASK
