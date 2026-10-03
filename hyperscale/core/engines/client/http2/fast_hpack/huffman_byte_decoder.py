from typing import List, Optional, Sequence, Tuple

# Flags of the nibble state machine's transitions (RFC 7541 Appendix B,
# nghttp2's table layout).
HUFFMAN_COMPLETE = 0x01
HUFFMAN_EMIT_SYMBOL = 0x02
HUFFMAN_FAIL = 0x04

NIBBLES_PER_STATE = 16
BYTE_VALUES = 256


class HuffmanByteDecoder:
    """
    Decodes HPACK Huffman strings (RFC 7541 5.2) one input byte per step.

    Each (state, byte) transition is composed from two steps of the nibble
    state machine the first time it is needed, then kept: real header text
    uses a few thousand of the 65,792 transitions, so the table never pays
    for the rest. A string must end in an accepting state -- its padding a
    prefix of EOS shorter than 8 bits -- and must not contain EOS; anything
    else is a decoding error.
    """

    __slots__ = (
        "_nibble_table",
        "_fail_state",
        "_accepting_states",
        "_transitions",
    )

    def __init__(self, nibble_table: Sequence[Tuple[int, int, int]]) -> None:
        state_count = len(nibble_table) // NIBBLES_PER_STATE

        self._nibble_table = nibble_table
        self._fail_state = state_count
        self._accepting_states = frozenset(
            [0]
            + [
                next_state
                for next_state, flags, _ in nibble_table
                if flags & HUFFMAN_COMPLETE and not flags & HUFFMAN_FAIL
            ]
        )
        self._transitions: List[Optional[Tuple[int, bytes]]] = [None] * ((state_count + 1) * BYTE_VALUES)

    def decode(self, encoded: bytes | bytearray) -> bytes:
        state = 0
        decoded_parts: List[bytes] = []
        append_part = decoded_parts.append
        transitions = self._transitions

        for input_byte in encoded:
            transition_index = state << 8 | input_byte
            state, emitted = transitions[transition_index] or self._compose_transition(transition_index)
            append_part(emitted)

        if state not in self._accepting_states:
            raise Exception("Invalid Huffman-encoded string")

        return b"".join(decoded_parts)

    def _compose_transition(self, transition_index: int) -> Tuple[int, bytes]:
        state = transition_index >> 8
        emitted = bytearray()

        if state != self._fail_state:
            for nibble in (transition_index >> 4 & 0x0F, transition_index & 0x0F):
                state, flags, symbol = self._nibble_table[state * NIBBLES_PER_STATE + nibble]

                if flags & HUFFMAN_FAIL:
                    state = self._fail_state
                    break

                if flags & HUFFMAN_EMIT_SYMBOL:
                    emitted.append(symbol)

        transition = self._transitions[transition_index] = (state, bytes(emitted))
        return transition
