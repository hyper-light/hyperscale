"""
Manager rate limiting coordinator (AD-24).

Provides per-client rate limiting with health-gated adaptive behavior,
integrating with the manager's HybridOverloadDetector.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from hyperscale.distributed.reliability.rate_limiting import (
    ServerRateLimiter,
    AdaptiveRateLimitConfig,
    RateLimitConfig,
    RateLimitResult,
    CooperativeRateLimiter,
)
from hyperscale.distributed.reliability.overload import HybridOverloadDetector
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.logging import Logger


class ManagerRateLimitingCoordinator:
    """
    Coordinates rate limiting for the manager server (AD-24).

    Provides:
    - Per-client rate limiting with adaptive behavior
    - Health-gated limiting (activates under stress)
    - Priority-aware request shedding during overload
    - Cooperative rate limit tracking for outbound requests
    - Integration with HybridOverloadDetector

    Key behaviors:
    - HEALTHY state: Per-operation limits apply
    - BUSY state: Low priority shed + per-operation limits
    - STRESSED state: Fair-share limiting per client
    - OVERLOADED state: Only critical requests pass
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        overload_detector: HybridOverloadDetector,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

        # Configure adaptive rate limiting
        adaptive_config = AdaptiveRateLimitConfig(
            window_size_seconds=60.0,
            default_max_requests=config.rate_limit_default_max_requests,
            default_window_size=config.rate_limit_default_window_seconds,
            operation_limits={
                # High-frequency operations
                "stats_update": (500, 10.0),
                "heartbeat": (200, 10.0),
                "progress_update": (300, 10.0),
                "worker_heartbeat": (200, 10.0),
                # Standard operations
                "job_submit": (50, 10.0),
                "job_status": (100, 10.0),
                "workflow_dispatch": (100, 10.0),
                "state_sync": (100, 10.0),
                # Infrequent operations
                "cancel": (20, 10.0),
                "reconnect": (10, 10.0),
                "register": (20, 10.0),
                # Default fallback
                "default": (100, 10.0),
            },
            stressed_requests_per_window=100,
            overloaded_requests_per_window=10,
            min_fair_share=10,
            max_tracked_clients=10000,
            inactive_cleanup_seconds=config.rate_limit_cleanup_interval_seconds,
        )

        # Server-side rate limiter (for incoming requests)
        self._server_limiter = ServerRateLimiter(
            overload_detector=overload_detector,
            adaptive_config=adaptive_config,
        )

        # Cooperative rate limiter (for outbound requests to gates/peers)
        self._cooperative_limiter = CooperativeRateLimiter(
            default_backoff=1.0,
        )

        # Metrics tracking
        self._cleanup_last_run: float = time.monotonic()
        self._cleanup_task: asyncio.Task | None = None

    def check_rate_limit(
        self,
        client_id: str,
        operation: str,
        priority: RequestPriority = RequestPriority.NORMAL,
    ) -> RateLimitResult:
        """
        Check if a request should be allowed based on rate limits.

        Args:
            client_id: Client identifier (usually node_id or address)
            operation: Operation type being performed
            priority: Priority level of the request

        Returns:
            RateLimitResult indicating if allowed
        """
        return self._server_limiter.check_rate_limit_with_priority(
            client_id,
            operation,
            priority,
        )

    async def check_rate_limit_async(
        self,
        client_id: str,
        operation: str,
        priority: RequestPriority = RequestPriority.NORMAL,
        max_wait: float = 0.0,
    ) -> RateLimitResult:
        """
        Async check with optional wait.

        Args:
            client_id: Client identifier
            operation: Operation type
            priority: Priority level
            max_wait: Maximum time to wait if rate limited

        Returns:
            RateLimitResult indicating if allowed
        """
        return await self._server_limiter.check_rate_limit_with_priority_async(
            client_id,
            operation,
            priority,
            max_wait=max_wait,
        )

    def check_simple(
        self,
        addr: tuple[str, int],
    ) -> bool:
        """
        Simple rate limit check for protocol compatibility.

        Args:
            addr: Source address tuple

        Returns:
            True if request is allowed
        """
        return self._server_limiter.check(addr)

    async def wait_if_outbound_limited(self, operation: str) -> float:
        """
        Wait if outbound operation is rate limited by server response.

        Args:
            operation: Operation type

        Returns:
            Time waited in seconds
        """
        return await self._cooperative_limiter.wait_if_needed(operation)

    def handle_rate_limit_response(
        self,
        operation: str,
        retry_after: float,
    ) -> None:
        """
        Handle rate limit response from remote server.

        Records the rate limit for cooperative backoff.

        Args:
            operation: Operation that was rate limited
            retry_after: Suggested retry time from server
        """
        self._cooperative_limiter.handle_rate_limit(operation, retry_after)

        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"Rate limited for operation '{operation}', retry after {retry_after:.2f}s",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def is_outbound_blocked(self, operation: str) -> bool:
        """Check if outbound operation is currently blocked."""
        return self._cooperative_limiter.is_blocked(operation)

    def get_outbound_retry_after(self, operation: str) -> float:
        """Get remaining time until outbound operation is unblocked."""
        return self._cooperative_limiter.get_retry_after(operation)

    def reset_client(self, client_id: str) -> None:
        """Reset rate limit state for a client."""
        self._server_limiter.reset_client(client_id)

    def cleanup_inactive_clients(self) -> int:
        """
        Remove rate limit state for inactive clients.

        Returns:
            Number of clients cleaned up
        """
        cleaned = self._server_limiter.cleanup_inactive_clients()

        if cleaned > 0:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Rate limit cleanup: removed {cleaned} inactive clients",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )

        return cleaned

    async def start_cleanup_loop(self) -> None:
        """Start periodic cleanup of inactive client rate limits."""
        if self._cleanup_task is not None:
            return

        async def cleanup_loop() -> None:
            interval = self._config.rate_limit_cleanup_interval_seconds
            while True:
                try:
                    await asyncio.sleep(interval)
                    self.cleanup_inactive_clients()
                except asyncio.CancelledError:
                    break
                except Exception as cleanup_error:
                    self._task_runner.run(
                        self._logger.log,
                        ServerWarning(
                            message=f"Rate limit cleanup error: {cleanup_error}",
                            node_host=self._config.host,
                            node_port=self._config.tcp_port,
                            node_id=self._node_id,
                        )
                    )

        self._cleanup_task = asyncio.create_task(cleanup_loop())

    async def stop_cleanup_loop(self) -> None:
        """Stop the cleanup loop."""
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    def get_metrics(self) -> dict:
        """Get rate limiting metrics."""
        server_metrics = self._server_limiter.get_metrics()
        cooperative_metrics = self._cooperative_limiter.get_metrics()

        return {
            "server": server_metrics,
            "cooperative": cooperative_metrics,
        }

    def get_client_stats(self, client_id: str) -> dict[str, float]:
        """Get available slots for all operations for a client."""
        return self._server_limiter.get_client_stats(client_id)

    @property
    def overload_detector(self) -> HybridOverloadDetector:
        """Get the underlying overload detector."""
        return self._server_limiter.overload_detector
