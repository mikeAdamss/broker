
class ConfigurationError(Exception):
    """ Raised where we are unable to load the configuration
    """

    def __init__(self, message):
        self.message = message


class IndistinctSchemaError(Exception):
    """ Raised where we are unable to identify a schema for a given message
    """

    def __init__(self, message):
        self.message = message

class SchemaError(Exception):
    """ Raised where a message does not correctly conform to it's chosen schema
    """

    def __init__(self, message):
        self.message = message