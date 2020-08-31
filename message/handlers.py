
import json
from typing import NamedTuple, List, Union

from errors import SchemaError

class CloudFunctionSchema(NamedTuple):
    id: str

class GSSCloudFunctionLookupSchema(NamedTuple):
    return_id: str
    target_function: CloudFunctionSchema
    send_time: str
    value_list: Union[ List[int], List[str], List[float] ]

class GSSCloudFunctionLookup(object):
    
    def __init__(self, message):

        # Bytes -> Dict for data
        self.data = message.data.decode('utf-8')

        try:
            self.attributes = message.attributes
        except Exception as e:
            raise SchemaError("Unable to acquire attributes of message") from e

        try:
            # Map the message a named tuple for validation/type safety
            self.message = GSSCloudFunctionLookupSchema(
                return_id = self.attributes["return_id"],
                send_time = self.attributes["send_time"],
                value_list = self.attributes["value_list"],
                target_function = CloudFunctionSchema(id=self.attributes["cloud_function_id"])
            )
        except Exception as e:
            raise SchemaError('Unable to map message to schema: "GSSCloudFunctionLookupSchema".') from e
        
    def handle(self):
        """
        Do the actual thing
        """
        print("I'm r handling!")

