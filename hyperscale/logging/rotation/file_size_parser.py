import re

class FileSizeParser:

    def __init__(self) -> None:      
        self._units = {
            "B": 1, 
            "KB": 2**10, 
            "MB": 2**20, 
            "GB": 2**30, 
            "TB": 2**40
        }


    def parse(self, file_size: str):
        file_size = file_size.upper()

        if not re.match(r'([KMGT]?B)', file_size):
            file_size = re.sub(
                r'([KMGT]?B)', 
                r' \1', 
                file_size
            )

        number, unit = [string.strip() for string in file_size.split()]

        return int(
            float(number) * self._units[unit]
        )