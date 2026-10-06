"""``RateLimitRetryResult`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""



class RateLimitRetryResult:
    """Result of a rate-limit-aware operation."""

    def __init__(
        self,
        success: bool,
        response: bytes | None,
        retries: int,
        total_wait_time: float,
        final_error: str | None = None,
    ):
        self.success = success
        self.response = response
        self.retries = retries
        self.total_wait_time = total_wait_time
        self.final_error = final_error
