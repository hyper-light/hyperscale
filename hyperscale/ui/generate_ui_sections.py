import math
import pathlib
from typing import List, Literal

from hyperscale.core.engines.client import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.models import TerminalMode
from hyperscale.ui.components.counter import Counter, CounterConfig
from hyperscale.ui.components.header import Header, HeaderConfig
from hyperscale.ui.components.multiline_text import MultilineText, MultilineTextConfig
from hyperscale.ui.components.progress_bar import ProgressBar, ProgressBarConfig
from hyperscale.ui.components.scatter_plot import PlotConfig, ScatterPlot
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.components.timer import Timer, TimerConfig
from hyperscale.ui.components.total_rate import TotalRate, TotalRateConfig

WorkflowConfig = list[
    dict[
        Literal[
            "workflow_name",
            "workflow_title",
            "duration_seconds",
            "duration_string",
            "vus",
            "graph_name",
        ],
        str | int,
    ]
]


def generate_ui_sections(
    workflows: List[Workflow],
    terminal_mode: TerminalMode = "full",
):
    workflow_configs: WorkflowConfig = [
        {
            "workflow_name": workflow.name.lower(),
            "workflow_title": workflow.name,
            "duration_seconds": TimeParser(workflow.duration).time,
            "duration_string": workflow.duration,
            "vus": workflow.vus,
            "graph_name": pathlib.Path(workflow.graph).name,
        }
        for workflow in workflows
    ]

    hyperscale_terminal_mode = "extended"
    if terminal_mode == "ci":
        hyperscale_terminal_mode = "compatability"

    text_components = [
        Text(
            f"run_message_display_{config['workflow_name']}",
            TextConfig(
                text="Initializing...",
                terminal_mode=hyperscale_terminal_mode,
            ),
            subscriptions=[f"update_run_message_{config['workflow_name']}"],
        )
        for config in workflow_configs
    ]

    text_components.insert(
        0,
        Text(
            "run_message_display_initializing",
            TextConfig(
                text="Initializing...",
            ),
            subscriptions=["update_run_message_initializing"],
        ),
    )

    multiline_text_components = [
        MultilineText(
            "workflow_metadata_initializing",
            MultilineTextConfig(
                text=["Gathering test config."],
                horizontal_alignment="right",
                color="hot_pink_3",
                terminal_mode=hyperscale_terminal_mode,
            ),
            subscriptions=["update_workflow_metadata"],
        )
    ]

    multiline_text_components.extend(
        [
            MultilineText(
                f"workflow_metadata_{config['workflow_name']}",
                MultilineTextConfig(
                    text=[
                        f"File: {config['graph_name']}",
                        f"Workflow: {config['workflow_title']}",
                        f"Duration: {config['duration_string']}",
                        f"VUs: {config['vus']}",
                    ],
                    horizontal_alignment="right",
                    color="hot_pink_3",
                    terminal_mode=hyperscale_terminal_mode,
                ),
                subscriptions=["update_workflow_metadata"],
            )
            for config in workflow_configs
        ]
    )

    return [
        Section(
            SectionConfig(height="xx-small", width="large"),
            components=[
                Header(
                    "header",
                    HeaderConfig(
                        header_text="hyperscale",
                        formatters={
                            "y": [
                                lambda letter, _: "\n".join(
                                    [" " + line for line in letter.split("\n")]
                                )
                            ],
                            "l": [
                                lambda letter, _: "\n".join(
                                    [
                                        line[:-1] if idx == 2 else line
                                        for idx, line in enumerate(letter.split("\n"))
                                    ]
                                )
                            ],
                            "e": [
                                lambda letter, idx: "\n".join(
                                    [
                                        line[1:] if idx < 2 else line
                                        for idx, line in enumerate(letter.split("\n"))
                                    ]
                                )
                                if idx == 9
                                else letter
                            ],
                        },
                        color="aquamarine_2",
                        attributes=["bold"],
                        terminal_mode=hyperscale_terminal_mode,
                    ),
                ),
            ],
        ),
        Section(
            SectionConfig(
                height="xx-small", width="small", vertical_alignment="center"
            ),
            components=multiline_text_components,
        ),
        Section(
            SectionConfig(
                width="small",
                height="smallest",
                left_border="|",
                top_border="-",
                bottom_border="-",
                max_height=3,
                horizontal_alignment="center",
                vertical_alignment="center",
            ),
            components=[
                ProgressBar(
                    f"run_progress_{config['workflow_name']}",
                    ProgressBarConfig(
                        total=math.floor(config["duration_seconds"]),
                        active_color="royal_blue",
                        failed_color="white",
                        complete_color="hot_pink_3",
                        terminal_mode=hyperscale_terminal_mode,
                    ),
                    subscriptions=[
                        f"update_run_progress_seconds_{config['workflow_name']}"
                    ],
                )
                for config in workflow_configs
            ],
        ),
        Section(
            SectionConfig(
                width="large",
                height="xx-small",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
                max_height=3,
                horizontal_alignment="center",
                vertical_alignment="center",
            ),
            components=text_components,
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
                horizontal_alignment="center",
                max_height=3,
            ),
            components=[
                Timer(
                    f"run_timer_{config['workflow_name']}",
                    TimerConfig(
                        color="aquamarine_2",
                        terminal_mode=hyperscale_terminal_mode,
                        horizontal_alignment="center",
                    ),
                    subscriptions=[f"update_run_timer_{config['workflow_name']}"],
                )
                for config in workflow_configs
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
                max_height=3,
                left_padding=1,
                right_padding=1,
                horizontal_alignment="center",
            ),
            components=[
                Counter(
                    f"executions_counter_{config['workflow_name']}",
                    CounterConfig(
                        unit="total actions", terminal_mode=hyperscale_terminal_mode
                    ),
                    subscriptions=[
                        f"update_total_executions_{config['workflow_name']}"
                    ],
                )
                for config in workflow_configs
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                right_border="|",
                top_border="-",
                bottom_border="-",
                max_height=3,
                horizontal_alignment="center",
            ),
            components=[
                TotalRate(
                    f"total_executions_{config['workflow_name']}",
                    TotalRateConfig(unit="aps", terminal_mode=hyperscale_terminal_mode),
                    subscriptions=[
                        f"update_total_executions_rate_{config['workflow_name']}"
                    ],
                )
                for config in workflow_configs
            ],
        ),
        Section(
            SectionConfig(
                width="large",
                height="medium",
                left_border="|",
                top_border="-",
                bottom_border="-",
                left_padding=4,
                right_padding=4,
                horizontal_alignment="center",
            ),
            components=[
                ScatterPlot(
                    f"executions_over_time_{config['workflow_name']}",
                    PlotConfig(
                        plot_name="Completions Per. Second",
                        x_axis_name="Time (sec)",
                        y_axis_name="Value",
                        line_color="aquamarine_2",
                        point_char="dot",
                        terminal_mode=hyperscale_terminal_mode,
                    ),
                    subscriptions=[f"update_execution_rates_{config['workflow_name']}"],
                )
                for config in workflow_configs
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="medium",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
                left_padding=2,
                right_padding=2,
                horizontal_alignment="center",
            ),
            components=[
                Table(
                    f"execution_stats_table_{config['workflow_name']}",
                    TableConfig(
                        headers={
                            "step": {
                                "default": "N/A",
                                "fixed": True,
                            },
                            "total": {
                                "default": 0,
                            },
                            "ok": {
                                "data_color": lambda value: "aquamarine_2"
                                if value > 0
                                else None,
                                "default": 0,
                            },
                            "err": {
                                "data_color": lambda value: "hot_pink_3"
                                if value > 0
                                else None,
                                "default": 0,
                            },
                        },
                        minimum_column_width=8,
                        border_color="aquamarine_2",
                        terminal_mode=hyperscale_terminal_mode,
                        table_format="simple",
                    ),
                    subscriptions=[f"update_execution_stats_{config['workflow_name']}"],
                )
                for config in workflow_configs
            ],
        ),
    ]
