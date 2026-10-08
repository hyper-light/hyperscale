"""
Every SWIM counter the code increments is a counter ``Metrics`` has.

``Metrics.increment`` used to ignore a name it had no field for, and 23
counters incremented across SWIM (join rejections, suppressed gossip,
retries, recoveries) silently never counted. It now raises; this pins
every literal name at once, so a typo fails here rather than at the first
increment in production.
"""

import dataclasses
import pathlib
import re

import hyperscale.distributed as distributed_package
from hyperscale.distributed.swim.core.metrics import Metrics

INCREMENT_CALL = re.compile(r"(?:_metrics\.increment|increment_metric)\(\s*\"([a-z_]+)\"")


def test_every_incremented_counter_exists() -> None:
    package_root = pathlib.Path(distributed_package.__file__).parent
    incremented = {
        name
        for source_path in package_root.rglob("*.py")
        for name in INCREMENT_CALL.findall(source_path.read_text())
    }
    counters = {metric_field.name for metric_field in dataclasses.fields(Metrics)}

    assert incremented
    assert incremented - counters == set()
