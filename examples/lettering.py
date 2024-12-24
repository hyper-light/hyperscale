from hyperscale.terminal.components.header.font.word import Word


def run():
    word = Word("hyperscale")

    print(
        word.to_ascii(
            formatter_set={
                "y": [
                    lambda letter, idx: "\n".join(
                        [" " + line for line in letter.split("\n")]
                    )
                    if idx == 1
                    else letter
                ]
            }
        )
    )


run()
