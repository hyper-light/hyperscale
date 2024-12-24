from hyperscale.terminal.components.header.font.word import Word


def run():
    word = Word("hyperscale")
    header_word = word.to_ascii(
        formatter_set={
            "y": [
                lambda letter, idx: "\n".join(
                    [" " + line for line in letter.split("\n")]
                )
                if idx == 1
                else letter
            ]
        },
    )

    print(header_word.ascii)
    print(header_word.height)
    print(header_word.width)


run()
