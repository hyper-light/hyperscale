import os
import secrets

from hyperscale.core.jobs.models import Env


AUTH_SECRET_ENVAR = "MERCURY_SYNC_AUTH_SECRET"

# 32 random bytes: the same strength as the per-user cluster cookie.
RUN_SECRET_BYTES = 32


def env_with_run_secret(env: Env) -> Env:
    """The ``Env`` a local run (``LocalRunner`` or ``ServerRunner``) and the
    worker processes it spawns share.

    A configured secret wins: ``env``'s own, else
    ``MERCURY_SYNC_AUTH_SECRET``. With neither, the run gets a secret of its
    own -- ``secrets.token_urlsafe(32)``, generated once per runner -- which
    is never published: the runner hands this same ``Env`` to its workers
    (``LocalServerPool.run_pool`` sends ``env.model_dump()`` to each worker
    process, which rebuilds it with ``Env(**worker_env)``), so only the
    runner and its own workers can authenticate each other's frames.
    """
    if env.MERCURY_SYNC_AUTH_SECRET:
        return env

    return env.model_copy(
        update={
            "MERCURY_SYNC_AUTH_SECRET": os.getenv(AUTH_SECRET_ENVAR)
            or secrets.token_urlsafe(RUN_SECRET_BYTES),
        },
    )
