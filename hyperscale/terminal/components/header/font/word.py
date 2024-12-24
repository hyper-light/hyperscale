from .letters import Letters
from typing import Callable, List
from .formatted_letter import FormattedLetter

Formatter = Callable[[str, int], str]
FormatterSet = List[Formatter]


class Word:
    def __init__(self, plaintext_word: str):
        self._plaintext_word = plaintext_word
        self._lettering = Letters()

    def to_ascii(
        self,
        formatter_set: dict[str, FormatterSet] | None = None,
    ):
        letters = [
            self._lettering.get_letter(char)
            for char in self._plaintext_word
            if char in self._lettering
        ]

        word_lines: list[str] = []

        for letter_idx, letter in enumerate(letters):
            if formatter_set:
                letter = self._apply_formatters(
                    letter_idx,
                    letter,
                    formatter_set,
                )

            for line_idx, letter_line in enumerate(letter.ascii.split("\n")):
                if line_idx >= len(word_lines):
                    word_lines.append(letter_line)

                else:
                    word_lines[line_idx] += letter_line

        return "\n".join(word_lines)

    def _apply_formatters(
        self,
        letter_idx: int,
        letter: FormattedLetter,
        formatter_set: dict[str, FormatterSet],
    ):
        formatters = formatter_set.get(letter.plaintext_letter, [])
        formatted_letter = letter.ascii

        for formatter in formatters:
            formatted_letter = formatter(formatted_letter, letter_idx)

        formatted_letter_lines = formatted_letter.split("\n")

        return FormattedLetter(
            plaintext_letter=letter.plaintext_letter,
            ascii=formatted_letter,
            height=len(formatted_letter_lines),
            width=max(
                [len(line) for line in formatted_letter_lines],
            ),
        )
