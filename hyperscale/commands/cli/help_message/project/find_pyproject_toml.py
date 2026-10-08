import importlib.metadata
import inspect
import json
from pathlib import Path
import tomllib as toml


PYPROJECT_TABLE_NAME = "project-paths"


def find_caller_relative_path_to_pyproject() -> dict:
    """
    The calling module's project metadata, as a pyproject.toml holds it.

    An installed project answers from its distribution's metadata: an
    installed package has no pyproject.toml above it, and the nearest one
    above a virtual environment is whichever project it sits in. An editable
    install's metadata is as old as the install, so it -- like code that is
    not installed -- answers from the pyproject.toml found by walking up from
    the source.
    """
    mod_name, _ = _find_caller_module_name_and_file()
    top_level_package = mod_name.partition(".")[0]
    distribution = next(
        (
            importlib.metadata.distribution(distribution_name)
            for distribution_name in importlib.metadata.packages_distributions().get(top_level_package, [])
        ),
        None,
    )
    if _answers_from_metadata(distribution):
        distribution_metadata = distribution.metadata
        return {
            "project": {
                "name": distribution_metadata["Name"],
                "version": distribution_metadata["Version"],
            }
        }

    pyproject_toml_path = _get_pyproject_toml_path()

    pyproject_toml = _parse_pyproject_toml(pyproject_toml_path)

    return pyproject_toml

def _answers_from_metadata(distribution: importlib.metadata.Distribution | None) -> bool:
    """Whether ``distribution`` -- the calling module's, if it is installed --
    answers from its metadata: installed, and not editable (PEP 660's
    direct_url.json)."""
    if distribution is None:
        return False

    direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
    return not direct_url.get("dir_info", {}).get("editable", False)


def _get_pyproject_toml_path() -> Path:
    """
    Tries to find the pyproject.toml relative to the caller of this module.
    """

    mod_name, caller_filename = _find_caller_module_name_and_file()

    if mod_name in ("inspect", "pydoc"):
        # inspect.getmembers() might be calling us, or maybe pydoc.
        # this makes things confusing, so just use the current working dir.
        # TODO: assert that these are the built-in modules!
        return _find_pyproject_by_parent_traversal(Path.cwd())

    if isinstance(caller_filename, str):
        working_file = Path(caller_filename)
        assert working_file.is_file()
        return _find_pyproject_by_parent_traversal(working_file.parent)

    if mod_name == "__main__":
        # No filename but the mod name is __main__? Assume this is an interactive
        # prompt; thus load from the current working directory
        return _find_pyproject_by_parent_traversal(Path.cwd())

    # cannot determine filename AAAANNDD mod_name is not __main__????
    raise Exception(
        f"unable to determine filename of calling module: {mod_name}"
    )


def _find_caller_module_name_and_file() -> tuple[str, str | None]:
    """
    Returns the module name of the first caller in the stack that DOESN'T from from this
    module -- namely, project_paths.
    """

    MODULE_EXCEPTIONS = (
        # Skip over any stack frames in THIS module
        __name__,
    )

    # Walk the frames directly: inspect.stack() builds a FrameInfo (reading
    # source and searching every loaded module) for every frame on the stack.
    frame = inspect.currentframe()
    try:
        # Crawl up the stack until we no longer find a caller in THIS module or any
        # excluded module (e.g., ignore calls within pathlib)
        while frame is not None:
            mod_name = frame.f_globals.get("__name__")
            if mod_name not in MODULE_EXCEPTIONS:
                assert isinstance(mod_name, str)
                filename = frame.f_globals.get("__file__")
                return mod_name, filename

            frame = frame.f_back

        raise RuntimeError(f"cannot find any caller outside of {__name__}")
    finally:
        # Remove a reference cycle caused by holding the frame
        # See: https://docs.python.org/3/library/inspect.html#the-interpreter-stack
        del frame


def _find_pyproject_by_parent_traversal(base: Path) -> Path:
    """
    Returns the path to pyproject.toml relative to the given base path.
    Traverses BACKWARDS starting from the base and going out of the parents.
    """
    for directory in [base, *base.resolve().parents]:
        candidate = directory / "pyproject.toml"
        if candidate.is_file():
            return candidate

    raise Exception(
        f"cannot find pyproject.toml within {base} or any of its parents"
    )



def _parse_pyproject_toml(pyproject_path: Path) -> dict[str, Path]:
    """
    Given a pyproject.toml, parses its texts and returns a dictionary of valid paths.
    """
    with pyproject_path.open('rb') as toml_file:
        return toml.load(toml_file)