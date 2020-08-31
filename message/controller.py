
from message.handlers import GSSCloudFunctionLookup
from typing import NamedTuple
from errors import IndistinctSchemaError

HANDLERS = {"gss_cloud_function_lookup": GSSCloudFunctionLookup}

class SchemaController:
    
    def __init__(self):
        self.handlers = HANDLERS

    def confirm(self, message):
        """
        For a given message, work out which handler to use.
        Return a message handler or throws as Exception
        """

        # Identify the handler then remove it from the message

        try:
            handler_id = message.attributes["handler"]
        except KeyError:
            raise IndistinctSchemaError('No "handler" field provided to ' \
                    'identify the required message handling') from e
        message.attributes.pop("handler")

        # Return the right handler
        return self.handlers[handler_id](message)