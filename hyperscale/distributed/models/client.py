"""
Client-side result models for HyperscaleClient.

These dataclasses represent the results returned to users when interacting
with the Hyperscale distributed system through the client API. They provide
a clean interface for accessing job, workflow, and reporter results.

This module is the wire namespace of the models below. Each lives in a
file of its own and is re-homed here -- its ``__module__`` set to this
module -- so its pickled form names this module, exactly as before the
split: mixed-version clusters keep talking and data written earlier
keeps loading.
"""

from .client_job_result import ClientJobResult
from .client_reporter_result import ClientReporterResult
from .client_workflow_dc_result import ClientWorkflowDCResult
from .client_workflow_result import ClientWorkflowResult

_WIRE_MODELS = (
    ClientReporterResult,
    ClientWorkflowDCResult,
    ClientWorkflowResult,
    ClientJobResult,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
