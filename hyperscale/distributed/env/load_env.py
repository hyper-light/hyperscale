import os
from pydantic import BaseModel
from typing import Callable, Dict, TypeVar, Union

from dotenv import dotenv_values

from .env import Env

T = TypeVar("T", bound=BaseModel)

PrimaryType = Union[str, int, bool, float, bytes]


def _values_from_process_environment(
    envars: Dict[str, Callable[[str], PrimaryType]],
) -> Dict[str, PrimaryType]:
    """The typed values of every known envar set (non-empty) in the process environment."""
    values: Dict[str, PrimaryType] = {}
    for envar_name, envar_type in envars.items():
        envar_value = os.getenv(envar_name)
        if envar_value:
            values[envar_name] = envar_type(envar_value)

    return values


def _typed_env_file_values(
    env_file_values: Dict[str, str | None],
    envars: Dict[str, Callable[[str], PrimaryType]],
) -> Dict[str, PrimaryType | str | None]:
    """Convert, in place, each env-file value whose name is a known envar to that envar's type."""
    for envar_name, envar_value in env_file_values.items():
        envar_type = envars.get(envar_name)
        if envar_type:
            env_file_values[envar_name] = envar_type(envar_value)

    return env_file_values


def _values_from_env_file(
    env_file: str,
    envars: Dict[str, Callable[[str], PrimaryType]],
) -> Dict[str, PrimaryType | str | None]:
    """The typed values of ``env_file``, or none when it is unnamed or absent."""
    if not (env_file and os.path.exists(env_file)):
        return {}

    return _typed_env_file_values(dotenv_values(dotenv_path=env_file), envars)


def _without_none_values(values: Dict[str, PrimaryType | str | None]) -> Dict[str, PrimaryType | str]:
    """``values`` minus the entries whose value is None, so model defaults apply."""
    return {name: value for name, value in values.items() if value is not None}


def load_env(default: type[Env], env_file: str = None, override: T | None = None) -> T:
    envars = default.types_map()

    if env_file is None:
        env_file = ".env"

    values: Dict[str, PrimaryType] = _values_from_process_environment(envars)
    values.update(_values_from_env_file(env_file, envars))

    if override:
        values.update(**override.model_dump(
            exclude_unset=True, 
            exclude_none=True,
        ))

        return type(override)(**_without_none_values(values))

    return default(**_without_none_values(values))
