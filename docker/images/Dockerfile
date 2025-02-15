ARG PYTHON_VERSION=3.13
ARG OS_BASE=bookworm
FROM python:${PYTHON_VERSION}-slim-${OS_BASE} AS base

ARG USERNAME=hyperscale
ARG USER_UID=1000
ARG USER_GID=$USER_UID

USER root

ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN pip install uv

RUN mkdir -p /hyperscale
WORKDIR /hyperscale
COPY pyproject.toml requirements.txt LICENSE README.md .version /hyperscale/
COPY hyperscale /hyperscale
RUN uv venv && \
    . /hyperscale/.venv/bin/activate && \
    uv pip install setuptools wheel && \
    uv pip install -r requirements.txt && \
    uv pip install .

RUN groupadd --gid $USER_GID $USERNAME \
&& useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
#
# [Optional] Add sudo support. Omit if you don't need to install software after connecting.
&& apt-get update \
&& apt-get install -y sudo \
&& echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
&& chmod 0440 /etc/sudoers.d/$USERNAME

USER hyperscale
