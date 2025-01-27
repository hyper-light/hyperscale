import socket
from ctypes import c_ulong
from time import sleep
from termios import TIOCOUTQ
from fcntl import ioctl


def bind_tcp_socket(host: str, port: int) -> socket.socket:
    family = socket.AF_INET

    if host and ":" in host:
        family = socket.AF_INET6

    sock = socket.socket(family, socket.SOCK_STREAM)

    try:
        sock.bind((host, port))

    except OSError:
        pass

    sock.setblocking(False)
    sock.set_inheritable(True)

    return sock


def bind_udp_socket(host: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    try:
        sock.bind((host, port))
        sock.close()

    except Exception:
        pass

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    
    try:
        sock.bind((host, port))
        sock.setblocking(False)
        sock.set_inheritable(True)

    except OSError:
        pass

    return sock
