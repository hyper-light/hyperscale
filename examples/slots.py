class Test:
    __slots__ = (
        "message",
        "alt_message",
    )

    def __init__(self) -> None:
        self.message = "hello"
        self.alt_message = "Hello again!"


test = Test()

print(test)
