from __future__ import annotations

import pathlib
from typing import Literal

from pydantic import BaseModel, StrictInt, model_validator

# "full": the live dashboard, on a terminal able to show it (otherwise a
# node falls back to "ci-safe"); "ci": the same frames without color;
# "ci-safe": a node's append-only plain ASCII summary lines; "disabled":
# no output of the UI's own.
TerminalMode = Literal["disabled", "ci", "ci-safe", "full"]


class HyperscaleConfig(BaseModel):
    # A plain path, not DirectoryPath: DirectoryPath rejects a directory that
    # does not exist yet before the validator below could create it.
    logs_directory: pathlib.Path | str = "logs/"
    server_port: StrictInt = 8790
    terminal_mode: TerminalMode = "full"

    @model_validator(mode="after")
    def validate_logs_directory(self) -> HyperscaleConfig:
        logs_directory_path = pathlib.Path(self.logs_directory).absolute().resolve()
        if logs_directory_path.exists() and not logs_directory_path.is_dir():
            raise ValueError(f"logs_directory {logs_directory_path} exists and is not a directory")

        logs_directory_path.mkdir(parents=True, exist_ok=True)

        self.logs_directory = str(logs_directory_path)

        return self
