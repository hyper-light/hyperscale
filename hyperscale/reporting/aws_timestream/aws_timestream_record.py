import time
from hyperscale.reporting.common.results_types import MetricType


class AWSTimestreamRecord:
    def __init__(
        self,
        metric_type: MetricType = None,
        metric_step: str = None,
        metric_workflow: str = None,
        metric_group: str = None,
        metric_name: str = None,
        metric_value: int | float = None,
        session_uuid: str = None,
    ) -> None:
        measure_value_type = "DOUBLE"

        if metric_type == "COUNT":
            measure_value_type = "BIGINT"

        self.time = str(int(round(time.time() * 1000)))
        self.time_unit = "MILLISECONDS"
        self.dimensions = [
            {"Name": "metric_type", "Value": metric_type},
            {"Name": "metric_workflow", "Value": metric_workflow},
            {"Name": "metric_group", "Value": metric_group},
            {"Name": "metric_name", "Value": metric_name},
            {"Name": "session_uuid", "Value": session_uuid},
        ]

        self.measure_name = f"{metric_workflow}_{metric_name}"
        if metric_step:
            self.measure_name = f"{metric_workflow}_{metric_step}_{metric_name}"
            self.dimensions.append({"Name": "step", "Value": metric_step})

        self.measure_value = str(metric_value)
        self.measure_value_type = measure_value_type

    def to_dict(self):
        return {
            "Time": self.time,
            "TimeUnit": self.time_unit,
            "Dimensions": self.dimensions,
            "MeasureName": self.measure_name,
            "MeasureValue": self.measure_value,
            "MeasureValueType": self.measure_value_type,
        }
