"""Define exceptions for StEWI."""


class StewiFormatError(Exception):
    def __init__(self, message=None):
        if message is None:
            from stewi.formats import format_dict
            message = ("Not a supported stewi format. \n"
                       f"Try {', '.join(format_dict.keys())}")
        self.message = message
        super().__init__(self.message)


class InventoryNotAvailableError(Exception):
    def __init__(self, inv=None, year=None, message=None):
        if message is None:
            message = ("Inventory not available for requested year")
            if inv:
                message = message.replace("Inventory", inv)
            if year:
                message = message.replace("requested year", str(year))
        self.message = message
        super().__init__(self.message)


class DataNotFoundError(Exception):
    def __init__(self, message=None):
        if message is None:
            message = ("Source data not found, download before proceeding")
        self.message = message
        super().__init__(self.message)
