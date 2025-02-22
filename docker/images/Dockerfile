ARG PYTHON_VERSION=3.13
ARG OS_BASE=bookworm
FROM python:${PYTHON_VERSION}-slim-${OS_BASE} AS base

ARG USERNAME=hyperscale
ARG USER_UID=1000
ARG USER_GID=$USER_UID

USER root

ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN apt update && \ 
    apt install -y libssl3 && \
    pip install uv && \
    uv pip install --system hyperscale uvloop

RUN groupadd --gid $USER_GID $USERNAME \
&& useradd --uid $USER_UID --gid $USER_GID -m $USERNAME


RUN mkdir /tests && \
    chown -R $USERNAME:$USER_UID /tests

WORKDIR /tests


USER hyperscale