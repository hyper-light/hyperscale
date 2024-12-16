from collections import OrderedDict, defaultdict
from typing import Dict, List, Tuple, Union

from .plot_config import PlotConfig
from .plot_point import PlotPoint

CompletionRateSet = Tuple[str, List[Union[int, float]]]


class ScatterPlot:
    def __init__(self, config: PlotConfig) -> None:
        self.config = config

        self.points: List[PlotPoint] = []
        self.session_table_rows: List[OrderedDict] = []

        self.tables = defaultdict(list)
        self.data = defaultdict(dict)
        self._graph_time_steps: List[int] = []
        self.common_table_rows: List[OrderedDict] = []
        self.common_table: Union[str, None] = None

        self.actions_and_tasks_table_rows: Dict[str, List[OrderedDict]] = defaultdict(
            list
        )
        self.actions_and_tasks_tables: Dict[str, str] = {}

    def get_next_frame(self):
        pass
        # scatter_plot = plotille.scatter(
        #     self._graph_time_steps[:graph_size],
        #     stage_completion_rates[:graph_size],
        #     width=120,
        #     height=10,
        #     y_min=self.config.y_min,
        #     y_max=self.config.y_max,
        #     x_min=self.config.y_min,
        #     x_max=int(round(self.config.x_max, 0)),
        #     linesep="\n",
        #     X_label=self.config.x_label,
        #     Y_label=self.config.y_label,
        #     lc=self.config.line_color,
        #     marker=PointChar.by_name(self.config.point_char),
        # )
