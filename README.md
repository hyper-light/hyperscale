
# <b>Hyperscale</b>
[![PyPI version](https://img.shields.io/pypi/v/hyperlight-hyperscale?color=blue)](https://pypi.org/project/hyperscale/)
[![License](https://img.shields.io/github/license/hyper-light/hyperscale)](https://github.com/hyper-light/hyperscale/blob/main/LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](https://github.com/hyper-light/hyperscale/blob/main/CODE_OF_CONDUCT.md)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/hyperlight-hyperscale?color=red)](https://pypi.org/project/hyperscale/)


| Package     | Hyperscale                                                      |
| ----------- | -----------                                                     |
| Version     | 0.5.5                                                           |
| Download    | https://pypi.org/project/hyperlight-hyperscale/                            | 
| Source      | https://github.com/hyper-light/hyperscale                       |
| Keywords    | performance, testing, async, distributed, graph, DAG, workflow  |

Hyperscale is a Python performance and scalable unit/integration testing framework that makes creating and running complex test workflows easy.

Write and orchestrate workflows as Python classes and decorator-wrapped async methods (called <b>actions</b>) using clients from Playwright and HTTP3 to GRPC and DTLS-UDP concurrently, connect to over thirty one different reporting options, and execute your tests the same locally and distributed via an intuitive CLI. Test execution statistics and performance are displayed via an intuitive terminal UI allowing for real-time feedback.

<br/>

___________

## <b> Why Hyperscale? </b>

Understanding how your application performs under load provides valuable insights, from catching issues with latency and memory usage to finding race condititions and infrastructure misconfiguration. However, performance test tools are often difficult to use and lack the ability to simulate complex user scenarios. As a <b>simulation framework</b> Hyperscale is different, built from the ground up to facilitate multi-client workflow-driven testing without sacrificing developer experience, speed, or effciency. The Hyperscale project follows these tenants:

<br/>

### __Speed and efficiency by default__ 

Whether running on your personal laptop or distributed across a cluster, Hyperscale is *fast*, capable of generating millions of requests or interactions per minute and without consuming excessive memory such that running in modern cloud environments becomes difficult.

<br/>

### __Run with ease anywhere__

Hyperscale just works, requiring no additional setup beyond a supported Python distribution. Options (like ready made containers) to make running Hyperscale in more challenging environments easy are readily provided and maintained, but never required. Test code requires *no changes* whether running locally or distributed, and CLI option or configuration tweaks are kept as few as possible. Hyperscale embraces carefully selected modern developer experience features like starter templates generation, includes useful debugging tools like one-off request/response checking and IP lookup, and enforced a minimal but flexible API. This means developers and engineers spend less time learning the framework and more time making their applications the best they can.

<br/>

### __Flexibility and and painless extensibility__
Hyperscale ships with support for HTTP, HTTP2, HTTP3, SMTP, TCP, UDP, and Websockets out of the box. GraphQL, GraphQL-HTTP2, GRPC, and Playwright are available as optional extras that can be installed via:

```bash
pip install "hyperlight-hyperscale[<extra_here>]"
```

You can use any installed and supported client concurrently in the same or an independent Workflow, allowing you to exercise each part of your application's stack to the fullest. Performing non-test work to accomplish setup is easy and only requires writing a workflow where no actions specify a client response return type.

Hyperscale offers JSON and CSV results reporting by default, with 29 additional reporters readily available as extra install options. All additional reporting options utilize the same config API and require no additional boilerplate. Specifying custom metrics only entails specifying the `Metric[T]` return type, and all reporters support customer metrics just like the default metrics Hyperscale provides. Writing your own reporter or client is as easy as defining
a class with a hadful of methods and specifying a few return types.

<br/>

___________

## <b>Requirements and Getting Started</b>

Hyperscale makes use of the latest and greatest Python features, and requires Python 3.11+. We also recommend installing the latest LTS version of OpenSSL if you wish to perform DTLS UDP testing or run tests over SSL encrypted connections.

<br/>

### __Installing__ 

To install Hyperscale run:
```
pip install hyperlight-hyperscale
```
Verify the installation was was successful by running the command:
```
hyperscale --help
```

which should output

![Output of the hyperscale --help command](https://github.com/hyper-light/hyperscale/blob/main/images/hyperscale_help.png?raw=true "Verifying Install")


> [!TIP]
> Hyperscale has been tested using and fully supports both `poetry` and `uv` package managers. We strongly recommend `uv` for its speed and ease of use.

<br/>

### __Running your first test__ 

Get started by running Hyperscale's:
```bash
# Note: test.py can also be any other file
hyperscale new test.py
```

which will output the following:

![Output of the hyperscale new test.py command](https://github.com/hyper-light/hyperscale/blob/main/images/hyperscale_new_workflow.png?raw=true "Creating a Workflow")

and generate the the test below in the specified `test.py` file:
```python
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 1000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
        )

```

Before running our test let's do two things. First, if on a Unix system, set the maximum number of open files above its current limit. This can be done
by running:

```
ulimit -n 256000
```

> [!IMPORTANT]
> You can provide any number when setting the file limit, but it must be more than the maximum number of `vus` specified in your `Workflow`.

Next, let's verify that [httpbin.org/get](https://httpbin.org/get) 
can actually be reached by running:
```bash
hyperscale ping http https://httpbin.org/get
```

which should output:

![Output of the hyperscale ping http https://httpbin.org/get command](https://github.com/hyper-light/hyperscale/blob/main/images/hyperscale_ping_httpbin.png?raw=true "Checking httpbin.org/get is reachable before running the test")

Awesome! Now let's run the test by executing:
```bash
hyperscale run test.py
```

which will output:

![The hyperscale run test.py command starting a test run](https://github.com/hyper-light/hyperscale/blob/main/images/hyperscale_run_start.png?raw=true "A test started by running the hyperscale run test.py command")

Hyperscale runs a independent "worker" server on each CPU core and uses all available CPUs by default, so it may take a few seconds while the worker servers connect. During this few seconds, Hyperscale will let 
you know how many workers have successfully connected thusfar:

![Waiting for worker servers to connect](https://github.com/hyper-light/hyperscale/blob/main/images/hyperscale_run_connecting.png?raw=true "Local worker servers connecting prior to starting the test run")

Once the servers have connected your test will start, during which you will see both static test-wide and real-time updating run statstics:

![Hyperscale running the Test workflow](https://github.com/hyper-light/hyperscale/blob/main/images/hyperscale_run.png?raw=true "Hyperscale running the Test workflow in our test.py file and displaying live statistics about the test run")

Live-updated statistics include actions-per-second (i.e. APS - how many individual actions Hyperscale completed, error or otherwise, per second), a scatter plot showing actions-per-second over time, total completed requests, total workflow duration, and per-action total/success/error stats. If multiple Workflows are present in the same Test file and executing concrrently, live statistics for each workflow will display for five-seconds and will cycle between all
concurrent workflows.

Once the run is complete you should see:

![Output of hyperscale from a completed workflow run](https://github.com/hyper-light/hyperscale/blob/main/images/hyperscale_run_complete.png?raw=true "A Complete Workflow Run")

You have officially created and run your first workflow!

<br/>

___________


## <b>Running in Docker</b>

Hyperscale offers a Docker image that allows you to create and run tests in any Docker compatible environment. To run the Hyperscale Docker image run:

```bash
docker pull hyperlightorg/hyperscale:latest
```

then execute commands from within the image via:

```bash
docker run -e COLUMNS=200 -e LINES=60 -v <TEST_DIR>:/tests hyperscale <ARGS_HERE>
```

> [!IMPORTANT]  
> The Hyperscale image runs using a non-root user (`hyperscale`) that has read and write permissions for *only* the `/tests` directory. You <b><i>must</i></b> mount the directory where you wish to create and run tests to the `/tests` path

> [!TIP]
> By default the Hyperscale UI is fully enabled when running inside the Docker image. Unfortunately, Docker's default terminal size for running commands in an image causes issues. We recommend passing `-e COLUMNS=200` and `-e LINES=60` as options to prevent UI issues. Alternatively, you may disable Hyperscale's UI by running `hyperscale run -q <TEST_NAME_AND_OTHER_ARGS>`.

</br>

___________

## <b>Development</b>

To setup your environment run the following script:

```bash
# Clone the repo
git clone https://github.com/hyper-light/hyperscale.git && \
cd hyperscale

# We personally recommend uv
pip install uv

uv venv && \
source .venv/bin/activate

# Install Hyperscale

# NOTE: To install ALL dependencies for ALL reporting and
# client options, uncomment the line below:

# uv pip install -r requirements.txt.dev

uv pip install -e .

```
___________

## <b>Clients and Reporters</b>

Below find a tables of Hyperscale's supported client and reporting options, as well as co-requisite dependencies (if any):
<br/>

### __Clients__ 

| Client           | Additional Install Option                                       |  Dependencies                 |
| -----------      | -----------                                                     |------------                   |
| HTTP             | N/A                                                             | N/A                           |
| HTTP2            | N/A                                                             | N/A                           |
| HTTP3 (unstable) | pip install hyperscale[http3]                                   | aioquic                       |
| SMTP             | N/A                                                             | N/A                           |
| TCP              | N/A                                                             | N/A                           |
| UDP              | N/A                                                             | N/A                           |
| Websocket        | N/A                                                             | N/A                           |
| GRPC             | pip install hyperscale[grpc]                                    | protobuf                      |
| GraphQL          | pip install hyperscale[graphql]                                 | graphql-core                  |
| GraphQL-HTTP2    | pip install hyperscale[graphql]                                 | graphql-core                  |
| Playwright       | pip install hyperscale[playwright] && playwright install        | playwright                    |


<br/>

### __Reporters__

| Reporter             | Additional Install Option                                 |  Dependencies                            |
| -----------          | -----------                                               |------------                              |
| AWS Lambda           | pip install hyperscale[aws]                               | boto3                                    |
| AWS Timestream       | pip install hyperscale[aws]                               | boto3                                    |
| Big Query            | pip install hyperscale[google]                            | google-cloud-bigquery                    |
| Big Table            | pip install hyperscale[google]                            | google-cloud-bigtable                    |
| Cassandra            | pip install hyperscale[cassandra]                         | cassandra-driver                         |
| Cloudwatch           | pip install hyperscale[aws]                               | boto3                                    |
| CosmosDB             | pip install hyperscale[azure]                             | azure-cosmos                             |
| CSV                  | N/A                                                       | N/A                                      |
| Datadog              | pip install hyperscale[datadog]                           | datadog                                  |
| DogStatsD            | pip install hyperscale[statsd]                            | aio_statsd                               |
| Google Cloud Storage | pip install hyperscale[google]                            | google-cloud-storage                     |
| Graphite             | pip install hyperscale[statsd]                            | aio_statsd                               |
| Honeycomb            | pip install hyperscale[honeycomb]                         | libhoney                                 |
| InfluxDB             | pip install hyperscale[influxdb]                          | influxdb_client                          |
| JSON                 | N/A                                                       | N/A                                      |
| Kafka                | pip install hyperscale[kafka]                             | aiokafka                                 |
| MongoDB              | pip install hyperscale[mongodb]                           | motor                                    |
| MySQL                | pip install hyperscale[sql]                               | aiomysql, sqlalchemy                     |
| NetData              | pip install hyperscale[statsd]                            | aio_statsd                               |
| New Relic            | pip install hyperscale[newrelic]                          | newrelic                                 |
| Postgresql           | pip install hyperscale[sql]                               | aiopg, psycopg2-binary, sqlalchemy       |
| Prometheus           | pip install hyperscale[prometheus]                        | prometheus-client, prometheus-client-api |
| Redis                | pip install hyperscale[redis]                             | redis, aioredis                          |
| S3                   | pip install hyperscale[aws]                               | boto3                                    |
| Snowflake            | pip install hyperscale[snowflake]                         | snowflake-connector-python, sqlalchemy   |
| SQLite3              | pip install hyperscale[sql]                               | sqlalchemy                               |
| StatsD               | pip install hyperscale[statsd]                            | aio_statsd                               |
| Telegraf             | pip install hyperscale[statsd]                            | aio_statsd                               |
| TelegrafStatsD       | pip install hyperscale[statsd]                            | aio_statsd                               |
| TimescaleDB          | pip install hyperscale[sql]                               | aiopg, psycopg2-binary, sqlalchemy       |
| XML                  | pip install hyperscale[xml]                               | dicttoxml                                |

<br/>

___________

## <b>Resources</b>

Hyperscale's official and full documentation is currently being written and will be linked here soon!

___________

## <b>License</b>

Hyper-Light and the Hyperscale project believe software should be truly open source forever. As such, this software is licensed under the MIT License in perpetuitiy. See the LICENSE file in the top distribution directory for the full license text.

___________

## <b>Contributing</b>

Hyperscale will be open to general contributions starting Fall, 2025 (once the distributed rewrite and general testing is complete). Until then, feel
free to use Hyperscale on your local machine and report any bugs or issues you find!

___________

## <b>Code of Conduct</b>

Hyperscale has adopted and follows the [Contributor Covenant code of conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md).
If you observe behavior that violates those rules please report to:

| Name            | Email                       | Twitter                                          |
|-------          |--------                     |----------                                        |
| Ada Lundhe      | alundhe@anaconda.com        | [@adalundhe.dev](https://bsky.app/profile/adalundhe.dev) |
