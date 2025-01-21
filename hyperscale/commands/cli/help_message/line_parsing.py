def is_arg_descriptor(line: str):
    stripped_line = line.strip()
    return stripped_line.startswith("@param") or stripped_line.startswith(":param")
