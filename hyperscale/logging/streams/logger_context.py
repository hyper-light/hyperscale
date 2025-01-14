import asyncio
import os

from typing import TypeVar
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
        )

    async def __aenter__(self):
        await self.stream.initialize()

        if self.stream._cwd is None:
            self.stream._cwd = await asyncio.to_thread(os.getcwd)

        if self.filename:
            await self.stream.open_file(
                self.filename,
                directory=self.directory,
                is_default=True,
                rotation_schedule=self.retention_policy,
            )

        if self.retention_policy and self.filename is None:
            filename = "logs.json"
            directory = os.path.join(self.stream._cwd, "logs")
            logfile_path = os.path.join(directory, filename)

            policy = RetentionPolicy(self.retention_policy)
            policy.parse()

            self.stream._retention_policies[logfile_path] = policy

        return self.stream

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
