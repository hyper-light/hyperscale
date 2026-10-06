"""``RateLimitRetryConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""



class RateLimitRetryConfig:
    """Configuration for rate limit retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        max_total_wait: float = 60.0,
        backoff_multiplier: float = 1.5,
    ):
        """
        Initialize retry configuration.

        Args:
            max_retries: Maximum number of retry attempts after rate limiting
            max_total_wait: Maximum total time to spend waiting/retrying (seconds)
            backoff_multiplier: Multiplier applied to retry_after on each retry
        """
        self.max_retries = max_retries
        self.max_total_wait = max_total_wait
        self.backoff_multiplier = backoff_multiplier
