import asyncio
import time
from random import randrange

import dill

from hyperscale.core_rewrite import Graph, Workflow, step
from hyperscale.core_rewrite.engines.client.http import HTTPResponse
from hyperscale.core_rewrite.engines.client.http2 import HTTP2Response
from hyperscale.core_rewrite.engines.client.shared.models import RequestType
from hyperscale.core_rewrite.results.workflow_results import WorkflowResults
from hyperscale.core_rewrite.testing import COUNT, URL, Metric


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

    @step()
    async def login(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("login")
    async def get_api_v1(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("login")
    async def get_api_v2(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("get_api_v1", "get_api_v2")
    async def check_statuses(
        self,
        login: HTTP2Response = None,
        get_api_v1: HTTP2Response = None,
        get_api_v2: HTTP2Response = None,
    ) -> Exception | None:
        assert (
            login.status >= 200 and login.status < 300
        ), f"Task One failed status check, got - {login.status}"
        assert (
            get_api_v1.status >= 200 and get_api_v1.status < 300
        ), f"Task Two failed status check, got - {get_api_v1.status}"
        assert (
            get_api_v2.status >= 200 and get_api_v2.status < 300
        ), f"Task Three failed status check, got - {get_api_v2.status}"

    @step("check_statuses")
    async def get_api_v1_failed(
        self,
        get_api_v2: HTTP2Response = None,
    ) -> Metric[COUNT]:
        return 1 if get_api_v2.status >= 400 else 0


async def run():
    w = Test()
    dill.dumps(w)

    g = Graph([w])

    print("CREATING DATA")

    responses = [
        HTTPResponse(
            "http://www.google.com",
            method="GET",
            status=200,
            status_message="OK",
            timings={
                "request_start": time.monotonic(),
                "connect_start": time.monotonic() + randrange(1, 10) / 100,
                "connect_end": time.monotonic() + randrange(11, 100) / 100,
                "write_start": time.monotonic() + randrange(101, 110) / 100,
                "write_end": time.monotonic() + randrange(111, 150) / 100,
                "read_start": time.monotonic() + randrange(151, 160) / 100,
                "read_end": time.monotonic() + randrange(160, 200) / 100,
                "request_end": time.monotonic() + randrange(201, 210) / 100,
            },
        )
        for _ in range(10**7)
    ]

    counts = [Metric(1, "COUNT") for _ in range(10**7)]

    samples = [Metric(randrange(1, 1000), "SAMPLE") for _ in range(10**7)]

    distributions = [Metric(randrange(1, 1000), "DISTRIBUTION") for _ in range(10**7)]

    rates = [Metric(1, "RATE") for _ in range(10**7)]

    errs = [Exception("Test err.") for _ in range(10**7)]

    print("DONE CREATING DATA.")
    print("RUNNING STATS BENCH.")

    res = WorkflowResults()

    start = time.monotonic()

    stats = res._process_http_or_udp_timings_set(
        "test",
        "test_step",
        RequestType.HTTP,
        responses,
    )

    res._process_exception_set(
        "test",
        "test_step",
        errs,
    )

    res._process_metrics_set(
        "test",
        "test_step",
        "COUNT",
        counts,
    )

    res._process_metrics_set(
        "test",
        "test_step",
        "SAMPLE",
        samples,
    )

    res._process_metrics_set(
        "test",
        "test_step",
        "DISTRIBUTION",
        distributions,
    )

    res._process_metrics_set(
        "test",
        "test_step",
        "RATE",
        rates,
    )

    elapsed = time.monotonic() - start

    print(elapsed)

    print(stats)

    # data = list(range(0, 10001))

    # print(np.percentile(data, [25, 50, 99]))

    # await g.setup()

    # await g.run()


loop = asyncio.new_event_loop()

loop.set_task_factory(asyncio.eager_task_factory)
asyncio.set_event_loop(loop)

loop.run_until_complete(run())
