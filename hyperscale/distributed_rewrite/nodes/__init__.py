"""
Distributed node implementations for Hyperscale.

This module provides Gate, Manager, Worker, and Client implementations.
Servers inherit from the SWIM HealthAwareServer for UDP healthchecks
and add TCP handlers for their specific role.

Architecture:
- Worker: Executes workflows, reports to managers via TCP
- Manager: Orchestrates workers within a DC, handles quorum
- Gate: Coordinates across DCs, manages leases
- Client: Submits jobs and receives push notifications
"""

from .worker import WorkerServer as WorkerServer
from .manager import ManagerServer as ManagerServer
from .gate import GateServer as GateServer
from .client import HyperscaleClient as HyperscaleClient

