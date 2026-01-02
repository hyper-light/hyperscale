"""
Distributed node implementations for Hyperscale.

This module provides Gate, Manager, and Worker node servers.
All nodes inherit from the SWIM UDPServer for UDP healthchecks
and add TCP handlers for their specific role.

Architecture:
- Worker: Executes workflows, reports to managers via TCP
- Manager: Orchestrates workers within a DC, handles quorum
- Gate: Coordinates across DCs, manages leases
"""

from .worker import WorkerServer as WorkerServer
from .manager import ManagerServer as ManagerServer
from .gate import GateServer as GateServer

