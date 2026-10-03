import ssl
from asyncio.sslproto import SSLProtocol, SSLProtocolState

# What asyncio's SSLProtocol treats as "no more data for now" on a read.
_SSL_AGAIN_ERRORS = (ssl.SSLWantReadError, ssl.SSLSyscallError)


class ClientSSLProtocol(SSLProtocol):
    """
    asyncio's SSLProtocol with leaner paths for the reads and writes every
    request makes.

    Reads: asyncio reads decrypted data until ``SSLObject.read`` raises
    ``SSLWantReadError``, so each TLS read ends in a raised and caught
    exception. Here the loop stops once OpenSSL holds no decrypted bytes
    (``SSLObject.pending()``) and the incoming BIO no encrypted ones --
    exactly when the next read could only raise. Python never enables OpenSSL
    read-ahead, so no whole record can sit unseen inside OpenSSL. A record
    without application data (a session ticket, a key update) still raises
    and is caught as before, and close_notify still ends the loop with an
    empty read.

    Writes: one buffer on an established connection, with nothing queued and
    writing not paused, is encrypted and sent directly, without passing
    through the write backlog. Anything else -- a partial or would-block
    write, a paused or closing connection, several buffers -- takes asyncio's
    path unchanged.
    """

    def _do_read__copied(self):
        chunk = b"1"
        zero = True
        one = False

        try:
            while True:
                chunk = self._sslobj.read(self.max_size)
                if not chunk:
                    break

                if zero:
                    zero = False
                    one = True
                    first = chunk
                elif one:
                    one = False
                    data = [first, chunk]
                else:
                    data.append(chunk)

                if not self._sslobj.pending() and not self._incoming.pending:
                    break

        except _SSL_AGAIN_ERRORS:
            pass

        if one:
            self._app_protocol.data_received(first)
        elif not zero:
            self._app_protocol.data_received(b"".join(data))

        if not chunk:
            # close_notify
            self._call_eof_received()
            self._start_shutdown()

    def _write_appdata(self, list_of_data):
        if (
            len(list_of_data) != 1
            or self._state is not SSLProtocolState.WRAPPED
            or self._write_backlog
            or self._ssl_writing_paused
        ):
            return super()._write_appdata(list_of_data)

        data = list_of_data[0]

        try:
            try:
                written = self._sslobj.write(data)

            except _SSL_AGAIN_ERRORS:
                written = 0

            if written < len(data):
                # Queue what OpenSSL did not take, as asyncio's path would.
                remainder = data[written:]
                self._write_backlog.append(remainder)
                self._write_buffer_size += len(remainder)
                self._process_outgoing()
                return

            if encrypted := self._outgoing.read():
                self._transport.write(encrypted)

            self._control_app_writing()

        except Exception as ex:
            self._fatal_error(ex, "Fatal error on SSL protocol")
