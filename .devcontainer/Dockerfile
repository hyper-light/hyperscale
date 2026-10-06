ARG UV_VERSION=0.11.2

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM corpheus91/devcontainers:python-3.12-bookworm

COPY --from=uv /uv /uvx /bin/

RUN apt-get update && \
    apt-get install -y libssl-dev

RUN apt install make gcc -y && \
    cd /usr/local/src && \
    wget https://www.openssl.org/source/openssl-1.1.1c.tar.gz && \
    tar xvf openssl-1.1.1c.tar.gz && \
    cd openssl-1.1.1c && \
    ./config -Wl,--enable-new-dtags,-rpath,'$(LIBRPATH)' && \
    make && \
    make install
