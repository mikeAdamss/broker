
import yaml
from typing import NamedTuple
from errors import ConfigurationError

class ConfigObj(NamedTuple):
    project_id: str
    subscription_id: str
    credential_path: str
    listen_timeout: int

def get_config(path_to_config_yaml="./config.yaml"):

    try:
        with open(path_to_config_yaml, 'r') as f:
            config_dict = yaml.safe_load(f)
    except Exception as e:
        raise ConfigurationError("Unable to load config.yaml from location: {}" \
            .format(path_to_config_yaml)) from e

    try:
        configObj = ConfigObj(
            project_id = config_dict["project_id"],
            subscription_id = config_dict["subscription_id"],
            credential_path = config_dict["credential_path"],
            listen_timeout = config_dict["listen_timeout"]
        )
    except Exception as e:
        raise ConfigurationError("Unable to instantiate config object. Does your yaml " \
                                "have the expected fields and types?") from e

    return configObj
