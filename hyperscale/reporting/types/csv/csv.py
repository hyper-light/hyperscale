import asyncio
import csv
import functools
import os
import signal
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, TextIO

import psutil


from hyperscale.reporting.metric.metrics_set import MetricsSet

from .csv_config import CSVConfig

has_connector = True


def handle_loop_stop(
    signame,
    executor: ThreadPoolExecutor,
    loop: asyncio.AbstractEventLoop,
    events_file: TextIO,
):
    try:
        events_file.close()
        executor.shutdown(wait=False, cancel_futures=True)
        loop.stop()
    except Exception:
        pass


class CSV:
    def __init__(self, config: CSVConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = Path(config.metrics_filepath).absolute()
        self.experiments_filepath = config.experiments_filepath
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        experiments_path = Path(self.experiments_filepath)
        experiments_directory = experiments_path.parent
        experiments_filename = experiments_path.stem

        self.variants_filepath = os.path.join(
            experiments_directory, f"{experiments_filename}_variants.csv"
        )

        self.mutations_filepath = os.path.join(
            experiments_directory, f"{experiments_filename}_mutations.csv"
        )

        self.streams_filepath = config.streams_filepath

        system_metrics_path = Path(config.system_metrics_filepath)
        system_metrics_directory = system_metrics_path.parent
        system_metrics_filename = system_metrics_path.stem

        self.stage_system_metrics_filepath = os.path.join(
            system_metrics_directory, f"{system_metrics_filename}_stages.csv"
        )

        self.session_system_metrics_filepath = os.path.join(
            system_metrics_directory, f"{system_metrics_filename}_session.csv"
        )

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self._events_csv_writer: csv.DictWriter = None
        self._metrics_csv_writer: csv.DictWriter = None
        self._stage_metrics_csv_writer: csv.DictWriter = None
        self._errors_csv_writer: csv.DictWriter = None
        self._custom_metrics_csv_writer: csv.DictWriter = None
        self._experiments_writer: csv.DictWriter = None
        self._variants_writer: csv.DictWriter = None
        self._mutations_writer: csv.DictWriter = None
        self._streams_writer: csv.DictWriter = None
        self._stage_system_metrics_writer: csv.DictWriter = None
        self._session_system_metrics_writer: csv.DictWriter = None

        self._loop: asyncio.AbstractEventLoop = None

        self.events_file: TextIO = None
        self.metrics_file: TextIO = None
        self.experiments_file: TextIO = None
        self.variants_file: TextIO = None
        self.mutations_file: TextIO = None
        self.streams_file: TextIO = None
        self.stage_system_metrics_file: TextIO = None
        self.session_system_metrics_file: TextIO = None

        self.write_mode = "w" if config.overwrite else "a"

        filepath = Path(config.metrics_filepath)
        base_filepath = filepath.parent
        base_filename = filepath.stem

        self.shared_metrics_filepath = os.path.join(
            base_filepath, f"{base_filename}_shared.csv"
        )

        self.custom_metrics_filepath = os.path.join(
            base_filepath, f"{base_filename}_custom.csv"
        )

        self.errors_metrics_filepath = os.path.join(
            base_filepath, f"{base_filename}_errors.csv"
        )

    async def connect(self):
        self._loop = asyncio._get_running_loop()
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Setting filepaths"
        )

        original_filepath = Path(self.events_filepath)

        directory = original_filepath.parent
        filename = original_filepath.stem

        events_file_timestamp = time.time()

        self.events_filepath = os.path.join(
            directory, f"{filename}_{events_file_timestamp}.csv"
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saving Shared Metrics to file - {self.metrics_filepath}"
        )

        headers = [
            "name",
            "stage",
            "group",
            "total",
            "succeeded",
            "failed",
            "actions_per_second",
        ]

        shared_metrics_file = await self._loop.run_in_executor(
            self._executor, functools.partial(open, self.shared_metrics_filepath, "w")
        )

        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame, self._executor, self._loop, shared_metrics_file
                ),
            )

        if self._stage_metrics_csv_writer is None:
            self._stage_metrics_csv_writer = csv.DictWriter(
                shared_metrics_file, fieldnames=headers
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(headers)}'
            )

            await self._loop.run_in_executor(
                self._executor, self._stage_metrics_csv_writer.writeheader
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(headers)}'
            )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            await self._loop.run_in_executor(
                self._executor,
                self._stage_metrics_csv_writer.writerow,
                {
                    "name": metrics_set.name,
                    "stage": metrics_set.stage,
                    "group": "common",
                    **metrics_set.common_stats,
                },
            )

        await self._loop.run_in_executor(self._executor, shared_metrics_file.close)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saved Shared Metrics to file - {self.metrics_filepath}"
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saving Metrics to file - {self.metrics_filepath}"
        )

        metrics_file = await self._loop.run_in_executor(
            self._executor, functools.partial(open, self.metrics_filepath, "w")
        )

        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame, self._executor, self._loop, metrics_file
                ),
            )

        for metrics_set in metrics:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            if self._metrics_csv_writer is None:
                headers = [*metrics_set.fields, "group"]

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(headers)}'
                )

                self._metrics_csv_writer = csv.DictWriter(
                    metrics_file, fieldnames=headers
                )

                await self._loop.run_in_executor(
                    self._executor, self._metrics_csv_writer.writeheader
                )

                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(headers)}'
                )

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                    f"{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}"
                )

                await self._loop.run_in_executor(
                    self._executor,
                    self._metrics_csv_writer.writerow,
                    {**group.record, "group": group_name},
                )

        await self._loop.run_in_executor(self._executor, metrics_file.close)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saved Metrics to file - {self.metrics_filepath}"
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saving Custom Metrics to file - {self.metrics_filepath}"
        )

        custom_metrics_file = None

        headers = [
            "name",
            "stage",
            "group",
        ]

        if self._custom_metrics_csv_writer is None:
            custom_metrics_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(open, self.custom_metrics_filepath, "w"),
            )

            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame, self._executor, self._loop, custom_metrics_file
                    ),
                )

            for metrics_set in metrics_sets:
                for custom_metric in metrics_set.custom_metrics.values():
                    headers.append(custom_metric.metric_name)

            self._custom_metrics_csv_writer = csv.DictWriter(
                custom_metrics_file, fieldnames=headers
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(headers)}'
            )

            await self._loop.run_in_executor(
                self._executor, self._custom_metrics_csv_writer.writeheader
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(headers)}'
            )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Custom Metrics Group - Custom"
            )

            await self._loop.run_in_executor(
                self._executor,
                self._custom_metrics_csv_writer.writerow,
                {
                    "name": metrics_set.name,
                    "stage": metrics_set.stage,
                    "group": "custom",
                    **{
                        custom_metric_name: custom_metric.metric_value
                        for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                    },
                },
            )

        await self._loop.run_in_executor(self._executor, custom_metrics_file.close)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saved Custom Metrics to file - {self.metrics_filepath}"
        )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saving Error Metrics to file - {self.metrics_filepath}"
        )

        error_csv_headers = ["name", "stage", "error_message", "error_count"]

        errors_file = await self._loop.run_in_executor(
            self._executor, functools.partial(open, self.errors_metrics_filepath, "w")
        )

        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame, self._executor, self._loop, errors_file
                ),
            )

        if self._errors_csv_writer is None:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(error_csv_headers)}'
            )
            error_csv_writer = csv.DictWriter(errors_file, fieldnames=error_csv_headers)

            await self._loop.run_in_executor(
                self._executor, error_csv_writer.writeheader
            )

            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(error_csv_headers)}'
            )

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio["hyperscale.reporting"].debug(
                f"{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}"
            )

            for error in metrics_set.errors:
                await self._loop.run_in_executor(
                    self._executor,
                    error_csv_writer.writerow,
                    {
                        "name": metrics_set.name,
                        "stage": metrics_set.stage,
                        "error_message": error.get("message"),
                        "error_count": error.get("count"),
                    },
                )

        await self._loop.run_in_executor(self._executor, errors_file.close)

        await self.logger.filesystem.aio["hyperscale.reporting"].info(
            f"{self.metadata_string} - Saved Error Metrics to file - {self.metrics_filepath}"
        )

    async def close(self):
        if self.events_file:
            await self._loop.run_in_executor(self._executor, self.events_file.close)

        if self.experiments_file:
            await self._loop.run_in_executor(
                self._executor, self.experiments_file.close
            )

        if self.variants_file:
            await self._loop.run_in_executor(self._executor, self.variants_file.close)

        if self.mutations_file:
            await self._loop.run_in_executor(self._executor, self.mutations_file.close)

        if self.streams_file:
            await self._loop.run_in_executor(self._executor, self.streams_file.close)

        if self.stage_system_metrics_file:
            await self._loop.run_in_executor(
                self._executor, self.stage_system_metrics_file.close
            )

        if self.session_system_metrics_file:
            await self._loop.run_in_executor(
                self._executor, self.session_system_metrics_file.close
            )

        self._executor.shutdown(wait=False, cancel_futures=True)
        await self.logger.filesystem.aio["hyperscale.reporting"].debug(
            f"{self.metadata_string} - Closing session - {self.session_uuid}"
        )
