class Timeouts:
    """
    A request/response client (HTTP, HTTP2, HTTP3, gRPC, GraphQL, WebSocket,
    TCP, UDP) bounds each section of a request -- connect, write, read -- by
    ``request_timeout``; a section that runs out ends the request at once
    with a timeout error. The per-phase timeouts bound the phases of the
    clients that move files (FTP, SFTP, SCP, SMTP).
    """

    __slots__ = (
        "connect_timeout",
        "read_timeout",
        "write_timeout",
        "request_timeout",
        "total_time",
    )

    def __init__(
        self,
        connect_timeout: int = 60,
        read_timeout: int = 45,
        write_timeout: int = 5,
        request_timeout: int = 60,
        total_time: int | None = None,
    ) -> None:
        if total_time is None:
            total_time = request_timeout

        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.write_timeout = write_timeout
        self.request_timeout = request_timeout
        self.total_time = total_time

        # No request outlasts the time the run has.
        if self.request_timeout > self.total_time:
            self.request_timeout = self.total_time
