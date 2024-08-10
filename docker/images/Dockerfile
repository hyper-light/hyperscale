ARG PYTHON_VERSION=3.12
ARG OS_BASE=bookworm
FROM python:${PYTHON_VERSION}-slim-${OS_BASE} AS base

ARG USERNAME=hyperscale
ARG USER_UID=1000
ARG USER_GID=$USER_UID

USER root

ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN apt-get update && \
    apt-get install -y gcc g++ curl git

ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh && \
    ln -s /root/.cargo/bin/uv /usr/local/bin/uv

ENV PATH="$PATH:/root/.cargo/bin"

RUN mkdir -p /app
WORKDIR /app
COPY pyproject.toml requirements.txt LICENSE README.md .version /app/
COPY hyperscale /app/hyperscale
RUN uv venv && \
    . /app/.venv/bin/activate && \
    uv pip install setuptools wheel && \
    uv pip install -r requirements.txt && \
    uv pip install . && \
    playwright install --with-deps

RUN groupadd --gid $USER_GID $USERNAME \
&& useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
#
# [Optional] Add sudo support. Omit if you don't need to install software after connecting.
&& apt-get update \
&& apt-get install -y sudo \
&& echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
&& chmod 0440 /etc/sudoers.d/$USERNAME

USER hyperscale
