import time


class AWSTimestreamErrorRecord:
    def __init__(
        self,
        record_name: str = None,
        record_stage: str = None,
        error_message: str = None,
        count: int = None,
        session_uuid: str = None,
    ) -> None:
        self.record_type = "error_metric"
        self.record_name = record_name

        self.time = str(int(round(time.time() * 1000)))
        self.time_unit = "MILLISECONDS"
        self.dimensions = [
            {"Name": "type", "Value": "error_metric"},
            {"Name": "name", "Value": record_name},
            {"Name": "stage", "Value": record_stage},
            {"Name": "error_message", "Value": error_message},
            {"Name": "session_uuid", "Value": session_uuid},
        ]
        self.measure_name = f"{record_name}_errors_{session_uuid}"
        self.measure_value = str(count)
        self.measure_value_type = "BIGINT"

    def to_dict(self):
        return {
            "Time": self.time,
            "TimeUnit": self.time_unit,
            "Dimensions": self.dimensions,
            "MeasureName": self.measure_name,
            "MeasureValue": self.measure_value,
            "MeasureValueType": self.measure_value_type,
        }
