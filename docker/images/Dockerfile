# syntax=docker/dockerfile:1
#
# The release image: the published hyperscale package, at exactly the
# version this commit's pyproject.toml declares, installed by uv as a
# tool (its own virtual environment, its CLI on PATH). Development
# clusters build docker/images/Dockerfile.source from source instead.
#
#   docker build -f docker/images/Dockerfile \
#     --build-arg HYPERSCALE_VERSION=<version> -t hyperscale .

ARG PYTHON_VERSION=3.13
ARG OS_BASE=bookworm
ARG UV_VERSION=0.11.2

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim-${OS_BASE}

# Required: an unpinned install could resolve to the release before
# this one while PyPI propagates the new upload.
ARG HYPERSCALE_VERSION
ARG USERNAME=hyperscale
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

COPY --from=uv /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON_PREFERENCE=only-system \
    UV_TOOL_DIR=/opt/uv-tools \
    UV_TOOL_BIN_DIR=/usr/local/bin \
    PYTHONUNBUFFERED=1

RUN test -n "${HYPERSCALE_VERSION}" || (echo "HYPERSCALE_VERSION build-arg is required" >&2 && exit 1)

RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/uv \
    uv tool install "hyperscale==${HYPERSCALE_VERSION}" --with uvloop

# The CLI reads its help metadata from the nearest pyproject.toml above
# the installed package (hyperscale/commands/cli/help_message/project),
# and fails to import without one; the tool environment's root is an
# ancestor of its site-packages.
COPY pyproject.toml /opt/uv-tools/hyperscale/pyproject.toml

RUN groupadd --gid ${USER_GID} ${USERNAME} \
    && useradd --uid ${USER_UID} --gid ${USER_GID} --create-home ${USERNAME} \
    && mkdir /tests \
    && chown -R ${USERNAME}:${USER_GID} /tests

WORKDIR /tests

USER ${USERNAME}
