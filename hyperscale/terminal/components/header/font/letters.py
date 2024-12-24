from typing import Literal
from .formatted_letter import FormattedLetter


SupportedLetters = Literal[
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
]


class Letters:
    def __init__(self):
        self._alphabet: dict[str, str] = {
            "a": """
              ____
              __// 
            //_//    
            """,
            "b": """
              //
             //_
            //_//
            """,
            "c": """
              ___
             //
            //__
            """,
            "d": """
                 //
             ___//
            //_//
            """,
            "e": """
              ____
             //_//
            //__
            """,
            "f": """
              ____
             //__
            //
            """,
            "g": """
              ____
             //_//
            ___//
            """,
            "h": """
              //
             //__
            // //
            """,
            "i": """
              @
             //
            //
            """,
            "j": """
                 //
                //
             __//
            """,
            "k": """
              //
             //__
            //\\\\
            """,
            "l": """
              //
             //
            //
            """,
            "m": """
              _______
             // // //
            // // //
            """,
            "n": """
              ____
             // //
            // //
            """,
            "o": """
              ____
             // //
            //_//
            """,
            "p": """
              ____
             //_//
            //
            """,
            "q": """
             ____
            //_//
              //
            """,
            "r": """
              ____
             // //
            //
            """,
            "s": """
              ____
             //__
            ___//
            """,
            "t": """
            __//__
             //
            //__
            """,
            "u": """
             // //
            //_//
            """,
            "v": """
            \\\\    //
             \\\\  //
              \\\\//
            """,
            "w": """
             \\\\  //\\\\  //
              \\\\//  \\\\//
            """,
            "x": """
            \\\\////
            ////\\\\
            """,
            "y": """
            _  _
            \\\\//
             //
            """,
            "z": """
            ___
             //
            //_""",
            "0": """
             ___
            || ||
            ||_||""",
            "1": """
             ___
              //
            _//_""",
            "2": """
            //\\\\
              //
             //_""",
            "3": """
             ____
             __//
            __//""",
            "4": """
            ||_||
               ||""",
            "5": """
             ____
            ||___
            ___//""",
            "6": """
             ___
            ||__
            ||_||""",
            "7": """
            ____
              //
             //
            """,
            "8": """
             ___
            ||_||
            ||_||
            """,
            "9": """
             ___
            ||_||
             __||
            """,
        }

    def __iter__(self):
        for plaintext_letter, ascii_letter in self._alphabet.items():
            yield (
                plaintext_letter,
                self._format_letter(
                    ascii_letter,
                    plaintext_letter,
                ),
            )

    def __contains__(self, plaintext_letter: str):
        return plaintext_letter in self._alphabet

    def get_letter(self, plaintext_letter: str):
        selected_letter = self._alphabet.get(plaintext_letter)

        if selected_letter is None:
            return selected_letter

        return self._format_letter(
            selected_letter,
            plaintext_letter,
        )

    def _format_letter(
        self,
        indented_ascii_letter: str,
        plaintext_letter: str,
    ):
        letter_lines = [
            line for line in indented_ascii_letter.split("\n") if len(line.strip()) > 0
        ]

        leading_spaces_count: list[int] = []

        for line in letter_lines:
            space_count = 0

            for char in line:
                if char == " ":
                    space_count += 1

                else:
                    leading_spaces_count.append(space_count)
                    break

        dedent_spaces = min(leading_spaces_count)

        dedented_lines: list[str] = []
        line_widths: list[int] = []

        for line in letter_lines:
            dedented_line = line[dedent_spaces:].rstrip()

            line_widths.append(len(dedented_line))
            dedented_lines.append(dedented_line)

        letter_width = max(line_widths)

        for idx, line in enumerate(dedented_lines):
            line_length = len(line)
            dedented_lines[idx] += " " * (letter_width - line_length)

        return FormattedLetter(
            plaintext_letter=plaintext_letter,
            ascii="\n".join(dedented_lines),
            height=len(dedented_lines),
            width=letter_width,
        )
