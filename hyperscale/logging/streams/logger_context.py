import asyncio
import os
from typing import Any, Literal, TypeVar

from hyperscale.logging.config.durability_mode import DurabilityMode

from .logger_stream import LoggerStream
from .retention_policy import (
    RetentionPolicy,
    RetentionPolicyConfig,
)


T = TypeVar("T")


class LoggerContext:
    def __init__(
        self,
        name: str | None = None,
        template: str | None = None,
        filename: str | None = None,
        directory: str | None = None,
        retention_policy: RetentionPolicyConfig | None = None,
        nested: bool = False,
        models: dict[
            str,
            tuple[
                type[T],
                dict[str, Any],
            ],
        ]
        | None = None,
        durability: DurabilityMode = DurabilityMode.FLUSH,
        log_format: Literal["json", "binary"] = "json",
        enable_lsn: bool = False,
        instance_id: int = 0,
    ) -> None:
        self.name = name
        self.template = template
        self.filename = filename
        self.directory = directory
        self.retention_policy = retention_policy
        self.stream = LoggerStream(
            name=name,
            template=template,
            filename=filename,
            directory=directory,
            retention_policy=retention_policy,
            models=models,
            durability=durability,
            log_format=log_format,
            enable_lsn=enable_lsn,
            instance_id=instance_id,
        )
        self.nested = nested

    async def __aenter__(self):
        await self.stream.initialize()

        if self.stream._cwd is None:
            loop = asyncio.get_event_loop()
            self.stream._cwd = await loop.run_in_executor(
                None,
                os.getcwd,
            )

        if self.filename:
            await self.stream.open_file(
                self.filename,
                directory=self.directory,
                is_default=True,
                retention_policy=self.retention_policy,
            )

        if self.retention_policy and self.filename is None:
            filename = "logs.json"
            cwd = self.stream._cwd if self.stream._cwd else os.getcwd()
            directory = os.path.join(cwd, "logs")
            logfile_path = os.path.join(directory, filename)

            policy = RetentionPolicy(self.retention_policy)
            policy.parse()

            self.stream._retention_policies[logfile_path] = policy

        return self.stream

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.nested is False:
            await self.stream.close(
                shutdown_subscribed=self.stream.has_active_subscriptions
            )
