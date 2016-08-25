
class Generator:
    """
    Base class for client implemented Generators.

    NOTE: This is basically just a useless interface at the moment,
    but we hypothesize that we'll add some functionality here in the
    future.
    """
    def __init__(self):
        self.properties = {}

    def set_properties(self, **kwargs):
        self.properties = kwargs

    def next_data(self):
        raise NotImplementedError()
