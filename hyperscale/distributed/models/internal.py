import msgspec
from typing import Literal


class Ack(msgspec.Struct):
    node: tuple[str, int]
    message: Literal['ACK'] = 'ACK'

class Confirm(msgspec.Struct):
    target: tuple[str, int]
    refuted: int
    required: int
    message: Literal['PROBE'] = 'PROBE'
    
class Eject(msgspec.Struct):
    target: tuple[str, int]
    confirmed: int
    message: Literal['EJECT'] = 'EJECT'

class Join(msgspec.Struct):
    udp_addr: tuple[str, int]
    tcp_addr: tuple[str, int]
    confirmed: int
    message: Literal['JOIN'] = 'JOIN'

class Leave(msgspec.Struct):
    node: tuple[str, int]
    message: Literal['LEAVE'] = 'LEAVE'

class Nack(msgspec.Struct):
    node: tuple[str, int]
    message: Literal['NACK'] = 'NACK'

class Probe(msgspec.Struct):
    target: tuple[str, int]
    comfirmed: int
    required: int
    message: Literal['PROBE'] = 'PROBE'
