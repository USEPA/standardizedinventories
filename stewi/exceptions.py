"""Define exceptions for StEWI."""


class StewiFormatError(Exception):
    def __init__(self, message=None):
        if message is None:
            from stewi.formats import format_dict
            message = ("Not a supported stewi format. \n"
                       f"Try {', '.join(format_dict.keys())}")
        self.message = message
        super().__init__(self.message)
