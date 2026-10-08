"""Every ``RetryAfterError`` survives a pickle round trip whole.

``Exception`` rebuilds an error from ``args`` alone, which dropped the retry
hint and broke subclasses whose constructors require more than a message.
"""

import pickle

import pytest

from hyperscale.distributed.nodes.gate.models.datacenter_refused_dispatch_error import (
    DatacenterRefusedDispatchError,
)
from hyperscale.distributed.nodes.gate.models.datacenters_without_room_error import (
    DatacentersWithoutRoomError,
)
from hyperscale.distributed.nodes.gate.models.transient_dispatch_error import TransientDispatchError
from hyperscale.distributed.reliability.retry_after_error import RetryAfterError


@pytest.mark.parametrize(
    "refusal",
    [
        RetryAfterError("refused", 1.25),
        TransientDispatchError("Not DC leader, retry at leader: unknown", 1.5),
        TransientDispatchError(None),
        DatacenterRefusedDispatchError("no room for another job", 2.0),
        DatacentersWithoutRoomError("no datacenter has room", 3.0, ("dc-east", "dc-west")),
    ],
    ids=lambda refusal: type(refusal).__name__,
)
def test_a_refusal_keeps_its_class_message_hint_and_fields(refusal: RetryAfterError) -> None:
    restored = pickle.loads(pickle.dumps(refusal))

    assert type(restored) is type(refusal)
    assert restored.args == refusal.args
    assert str(restored) == str(refusal)
    assert vars(restored) == vars(refusal)
