from __future__ import annotations

from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.resources.datacenter_resource_view import (
    DatacenterResourceView,
)
from hyperscale.distributed.resources.manager_resource_gossip_entry import (
    ManagerResourceGossipEntry,
)
from hyperscale.distributed.resources.manager_resource_gossip_message import (
    ManagerResourceGossipMessage,
)
from hyperscale.distributed.resources.manager_resource_report import (
    ManagerResourceReport,
)


class ManagerResourceGossip:
    """
    AD-41 Part 4 on the manager tier: every manager holds its datacenter's
    resource view -- the one a gate computes -- so a client that runs jobs
    on one datacenter without a gate sees it too.

    * Local view: this manager's ``ManagerResourceReport``, the report its
      gates receive: the workload of the workflows it leads (a partition of
      the datacenter's, so reports sum) and the datacenter's worker
      capacity.
    * Peer views: its peers' reports, by gossip. Each round a manager sends
      each peer every fresh report it holds -- its own and those it heard --
      with how long ago each one's manager made it. A datacenter's managers
      are few, so every message stays small while a report reaches every
      manager in one round, and a manager still learns the report of a peer
      it cannot reach directly. A dead manager's report ages from when it
      was made wherever it was passed, so forwarding never keeps it alive.
    * Cluster view: the same ``DatacenterResourceAggregator`` a gate runs,
      over every fresh report.
    """

    __slots__ = ("_datacenter", "_own_address", "_aggregator")

    def __init__(
        self,
        datacenter: str,
        own_address: tuple[str, int],
        aggregator: DatacenterResourceAggregator,
    ) -> None:
        self._datacenter = datacenter
        self._own_address = own_address
        self._aggregator = aggregator

    def record_own_report(self, report: ManagerResourceReport) -> None:
        """Hold this manager's report, made now."""
        self._aggregator.record(
            self._datacenter, self._own_address, report, age_seconds=0.0
        )

    def gossip_message(self) -> ManagerResourceGossipMessage:
        """Every fresh report held, for the peers."""
        return ManagerResourceGossipMessage(
            datacenter=self._datacenter,
            entries=[
                ManagerResourceGossipEntry(
                    manager_address=manager_address,
                    report=report,
                    age_seconds=age_seconds,
                )
                for manager_address, report, age_seconds in self._aggregator.fresh_reports(
                    self._datacenter
                )
            ],
        )

    def receive(self, message: ManagerResourceGossipMessage) -> bool:
        """Take a peer's gossip; False when it is another datacenter's. This
        manager's own report is never taken second-hand: it holds the
        freshest."""
        if message.datacenter != self._datacenter:
            return False

        for entry in message.entries:
            if entry.manager_address != self._own_address:
                self._aggregator.record(
                    self._datacenter,
                    entry.manager_address,
                    entry.report,
                    age_seconds=entry.age_seconds,
                )
        return True

    def view(self) -> DatacenterResourceView | None:
        """The datacenter's resource view, or None until a fresh report
        knows its capacity."""
        return self._aggregator.view(self._datacenter)
