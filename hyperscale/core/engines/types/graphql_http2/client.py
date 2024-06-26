import asyncio
import time
import uuid
from typing import Any, Coroutine, Optional, Union

from hyperscale.core.engines.types.common.timeouts import Timeouts
from hyperscale.core.engines.types.http2.client import MercuryHTTP2Client
from hyperscale.core.engines.types.tracing.trace_session import Trace, TraceSession

from .action import GraphQLHTTP2Action
from .result import GraphQLHTTP2Result


class MercuryGraphQLHTTP2Client(
    MercuryHTTP2Client[GraphQLHTTP2Action, GraphQLHTTP2Result]
):
    def __init__(
        self,
        concurrency: int = 10**3,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
        tracing_session: Optional[TraceSession] = None,
    ) -> None:
        super(MercuryGraphQLHTTP2Client, self).__init__(
            concurrency=concurrency,
            timeouts=timeouts,
            reset_connections=reset_connections,
            tracing_session=tracing_session,
        )

        self.session_id = str(uuid.uuid4())

    async def execute_prepared_request(
        self, action: GraphQLHTTP2Action
    ) -> Coroutine[Any, Any, GraphQLHTTP2Result]:
        trace: Union[Trace, None] = None
        if self.tracing_session:
            trace = self.tracing_session.create_trace()
            await trace.on_request_start(action)

        response = GraphQLHTTP2Result(action)
        response.wait_start = time.monotonic()
        self.active += 1

        if trace and trace.on_connection_queued_start:
            await trace.on_connection_queued_start(trace.span, action, response)

        async with self.sem:
            if trace and trace.on_connection_queued_end:
                await trace.on_connection_queued_end(trace.span, action, response)

            pipe = self.pool.pipes.pop()
            connection = self.pool.connections.pop()

            try:
                if action.hooks.listen:
                    event = asyncio.Event()
                    action.hooks.channel_events.append(event)
                    await event.wait()

                if action.hooks.before:
                    action: GraphQLHTTP2Action = await self.execute_before(action)
                    action.setup()

                response.start = time.monotonic()

                if trace and trace.on_connection_create_start:
                    await trace.on_connection_create_start(trace.span, action, response)

                stream = await connection.connect(
                    action.url.hostname,
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    ssl=action.ssl_context,
                    timeout=self.timeouts.connect_timeout,
                )

                stream.encoder = action.hpack_encoder

                response.connect_end = time.monotonic()

                if trace and trace.on_connection_create_end:
                    await trace.on_connection_create_end(trace.span, action, response)

                pipe.send_request_headers(action, stream)

                if trace and trace.on_request_headers_sent:
                    await trace.on_request_headers_sent(trace.span, action, response)

                if action.encoded_data is not None:
                    await pipe.submit_request_body(action, stream)

                response.write_end = time.monotonic()

                if action.encoded_data and trace and trace.on_request_data_sent:
                    await trace.on_request_data_sent(trace.span, action, response)

                await asyncio.wait_for(
                    pipe.receive_response(action, response, stream, trace),
                    timeout=self.timeouts.total_timeout,
                )

                response.complete = time.monotonic()

                if action.hooks.after:
                    response: GraphQLHTTP2Result = await self.execute_after(
                        action, response
                    )
                    action.setup()

                if action.hooks.notify:
                    await asyncio.gather(
                        *[
                            asyncio.create_task(
                                channel.call(response, action.hooks.listeners)
                            )
                            for channel in action.hooks.channels
                        ]
                    )

                    for listener in action.hooks.listeners:
                        if len(listener.hooks.channel_events) > 0:
                            listener.setup()
                            event = listener.hooks.channel_events.pop()
                            if not event.is_set():
                                event.set()

                self.pool.pipes.append(pipe)
                self.pool.connections.append(connection)

            except Exception as e:
                response.complete = time.monotonic()
                response._status = 400
                response.error = str(e)

                self.pool.reset()

                if trace and trace.on_request_exception:
                    await trace.on_request_exception(response)

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:
                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            if trace and trace.on_request_end:
                await trace.on_request_end(response)

            return response
