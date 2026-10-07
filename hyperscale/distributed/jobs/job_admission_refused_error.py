"""``JobAdmissionRefusedError`` -- raised when admission control refuses a submission."""


class JobAdmissionRefusedError(Exception):
    """Admission control (D-65 caps, D-67 noisy-job breaker) refused a job
    submission. ``ack`` is the serialized ``JobAck`` refusal -- carrying
    ``retry_after_seconds`` -- to answer the submitter with.

    Raised from inside the manager's submission decision so the refusal
    leaves through the decision's single failure exit, which releases the
    submission's idempotency reservation: a refusal to retry is not the
    key's final answer.
    """

    def __init__(self, ack: bytes, reason: str) -> None:
        super().__init__(reason)
        self.ack = ack
