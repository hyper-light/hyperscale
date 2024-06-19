FROM corpheus91/devcontainers:python-3.12-bookworm


RUN apt-get update && \
    apt-get install -y libssl-dev && \
    pip install uv

RUN apt install make gcc -y && \
    cd /usr/local/src && \
    wget https://www.openssl.org/source/openssl-1.1.1c.tar.gz && \
    tar xvf openssl-1.1.1c.tar.gz && \
    cd openssl-1.1.1c && \
    ./config -Wl,--enable-new-dtags,-rpath,'$(LIBRPATH)' && \
    make && \
    make install