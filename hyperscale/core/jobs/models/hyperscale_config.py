from __future__ import annotations

import pathlib
from typing import Literal

from pydantic import BaseModel, DirectoryPath, StrictInt, model_validator

TerminalMode = Literal["disabled", "ci", "full"]


class HyperscaleConfig(BaseModel):
    logs_directory: DirectoryPath = "logs/"
    server_port: StrictInt = 8790
    terminal_mode: TerminalMode = "full"

    @model_validator(mode="after")
    def validate_logs_directory(self) -> HyperscaleConfig:
        logs_directory_path = self.logs_directory
        if isinstance(logs_directory_path, str):
            logs_directory_path = pathlib.Path(self.logs_directory)

        logs_directory_path = logs_directory_path.absolute().resolve()

        if not logs_directory_path.exists():
            logs_directory_path.mkdir()

        self.logs_directory = str(logs_directory_path)

        return self
