import uuid
from typing import Dict, Union

from hyperscale.reporting.metric.custom_metric import CustomMetric


class MetricsGroup:
    def __init__(
        self,
        name: str,
        source: str,
        stage: str,
        group_name: str,
        data: Dict[str, Union[int, float, str, bool]],
        common: Dict[str, int],
    ) -> None:
        self.metrics_group_id = str(uuid.uuid4())

        self.name = name
        self.stage = stage
        self.group_name = group_name
        self.data = []

        self.fields = ["name", "stage"]
        self.values = [name, stage]

        self._additional_fields = ["quantiles", "errors", "custom_metrics"]
        self.source = source
        self._raw_data = data
        self._common = common

        flattened_data = dict(
            {
                field: value
                for field, value in data.items()
                if field not in self._additional_fields
            }
        )

        metrics_data = dict(flattened_data.get(group_name, {}))

        for metric_name, metric_value in metrics_data.items():
            self.data.append((metric_name, metric_value))

            self.fields.append(metric_name)
            self.values.append(metric_value)

        for quantile, quantile_value in data.get("quantiles", {}).items():
            self.data.append((quantile, quantile_value))

            self.fields.append(quantile)
            self.values.append(quantile_value)

        self.unique = list(self.data)
        self.unique_fields = list(self.fields)
        self.unique_values = list(self.values)

        self.custom_fields: Dict[str, CustomMetric] = data.get("custom_metrics", {})

    def get(self, field_name: str):
        return self._raw_data.get(field_name)

    @property
    def custom(self):
        return {
            custom_metric.metric_name: custom_metric.metric_value
            for custom_metric in self.custom_fields.values()
        }

    @property
    def custom_schemas(self):
        return {
            custom_metric.metric_name: custom_metric.metric_type
            for custom_metric in self.custom_fields.values()
        }

    @property
    def custom_field_names(self):
        return list(self.custom_fields.keys())

    @property
    def record(self):
        record_data = {f"{field}": value for field, value in self.data}

        custom_field_data = {f"{field}": value for field, value in self.custom.items()}

        return {
            "name": self.name,
            "stage": self.stage,
            "group": self.group_name,
            **record_data,
            **custom_field_data,
        }

    @property
    def raw_data(self):
        return self._raw_data

    @property
    def stats(self):
        stats_metrics = {}
        for metric_name, metric_value in self.data:
            if isinstance(metric_value, (int, float)):
                stats_metrics[metric_name] = metric_value

        return stats_metrics

    @property
    def quantiles(self):
        return self._raw_data.get("quantiles", {})
