from typing import List, Optional, Sequence, Tuple

# Flags of the nibble state machine's transitions (RFC 7541 Appendix B,
# nghttp2's table layout).
HUFFMAN_COMPLETE = 0x01
HUFFMAN_EMIT_SYMBOL = 0x02
HUFFMAN_FAIL = 0x04

NIBBLES_PER_STATE = 16
BYTE_VALUES = 256

# A row's slots after its 256 transitions: whether a string may end in the
# row's state, and the state itself.
ACCEPTING_SLOT = BYTE_VALUES
STATE_SLOT = BYTE_VALUES + 1

# A state's row: for each input byte, the transition -- the next state's row
# and the text emitted -- or None until composed; then the two slots above.
HuffmanRow = List[Optional[Tuple[list, str]] | bool | int]
HuffmanTransition = Tuple[HuffmanRow, str]


class HuffmanByteDecoder:
    """
    Decodes HPACK Huffman strings (RFC 7541 5.2) one input byte per step.

    Each state of the nibble state machine has a row, and ``row[byte]`` is
    the transition for an input byte: the next state's row itself and the
    text emitted, composed from two nibble steps the first time it is needed
    and then kept. A step is one list lookup with no arithmetic, and real
    header text composes a few thousand of the 65,792 transitions, so the
    rows never pay for the rest.

    Text is latin-1, one character per octet: output that is ASCII is the
    header text itself. A string must end in a row whose
    ``row[ACCEPTING_SLOT]`` is set -- its padding a prefix of EOS shorter than
    8 bits -- and must not contain EOS; a failed transition leads to a row
    that only leads back to itself and does not accept.
    """

    __slots__ = (
        "_nibble_table",
        "_fail_state",
        "_rows",
        "start_row",
    )

    def __init__(self, nibble_table: Sequence[Tuple[int, int, int]]) -> None:
        state_count = len(nibble_table) // NIBBLES_PER_STATE
        accepting_states = frozenset(
            [0]
            + [
                next_state
                for next_state, flags, _ in nibble_table
                if flags & HUFFMAN_COMPLETE and not flags & HUFFMAN_FAIL
            ]
        )

        self._nibble_table = nibble_table
        self._fail_state = state_count

        # The nibble machine's states, then the failure state.
        self._rows: List[HuffmanRow] = [
            [None] * BYTE_VALUES + [state in accepting_states, state]
            for state in range(state_count + 1)
        ]
        self.start_row: HuffmanRow = self._rows[0]

    def decode_text(self, encoded: bytes | bytearray) -> str:
        """The octets ``encoded`` decodes to, as latin-1 text."""
        row = self.start_row
        compose = self.compose
        text = ""

        for input_byte in encoded:
            row, fragment = row[input_byte] or compose(row, input_byte)
            text += fragment

        if not row[ACCEPTING_SLOT]:
            raise Exception("Invalid Huffman-encoded string")

        return text

    def compose(self, row: HuffmanRow, input_byte: int) -> HuffmanTransition:
        """Composes, keeps and returns ``row``'s transition for ``input_byte``."""
        state = row[STATE_SLOT]
        emitted = bytearray()

        if state != self._fail_state:
            for nibble in (input_byte >> 4, input_byte & 0x0F):
                state, flags, symbol = self._nibble_table[state * NIBBLES_PER_STATE + nibble]

                if flags & HUFFMAN_FAIL:
                    state = self._fail_state
                    break

                if flags & HUFFMAN_EMIT_SYMBOL:
                    emitted.append(symbol)

        transition = row[input_byte] = (self._rows[state], emitted.decode("latin-1"))
        return transition
