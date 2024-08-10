import sys as _sys
import platform as _platform

# isort: off
if _platform.system() == 'Windows':
    from blessed.win_terminal import Terminal as Terminal
else:
    from blessed.terminal import Terminal as Terminal  # type: ignore

