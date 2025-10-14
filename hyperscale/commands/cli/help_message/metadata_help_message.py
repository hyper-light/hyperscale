from pydantic import BaseModel, StrictInt
from .project import (
    PyProjectToml,
)
from hyperscale.ui.styling import stylize, get_style

from .cli_style import CLIStyle

class MetadataHelpMessage(BaseModel):
    pyproject_toml: PyProjectToml
    indentation: StrictInt = 0
    styling: CLIStyle | None = None

    async def to_message(
        self,
        global_styles: CLIStyle | None = None,
    ):
        indentation = self.indentation
        if global_styles and global_styles.indentation:
            indentation = global_styles.indentation

        styles = self.styling
        if styles is None:
            styles = global_styles

        version = await stylize(
            f'v{self.pyproject_toml.project.version}',
            color=get_style(styles.flag_color),
            highlight=get_style(styles.flag_highlight),
            attrs=get_style(styles.flag_attributes),
            mode=styles.to_mode(),
        )

        indentation = " " * max(indentation - 1, 0)
        metadata_message = '\n'.join([
            f'{indentation}{version}'
        ])

        return f'{metadata_message}\n'

