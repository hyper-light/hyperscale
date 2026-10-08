"""`--help` prints whole option descriptions and no escape codes off a terminal.

Option descriptions were cut with ``str.strip("@param")``, which drops any of
those characters from both ends ("in the same order" printed "orde"), and the
help always began by clearing the screen, which corrupts piped or CI output.
"""

import subprocess
import sys

HELP_PROBE = "import sys; from hyperscale.commands.root import run; sys.argv = ['hyperscale', 'run', 'manager', '--help']; run()"


def manager_help() -> str:
    completed = subprocess.run(
        [sys.executable, "-c", HELP_PROBE],
        capture_output=True,
        text=True,
        env={"PATH": "", "TERM": "xterm-256color"},
    )
    return completed.stdout + completed.stderr


def test_help_written_to_a_pipe_has_no_escape_codes() -> None:
    assert "\x1b[" not in manager_help()


def test_option_descriptions_keep_their_last_word() -> None:
    help_text = manager_help()

    assert "The UDP host:port of the same managers, in the same order" in help_text
    assert "in the same orde\n" not in help_text
