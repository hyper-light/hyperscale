import os
import json

from hyperscale.core.jobs.models import HyperscaleConfig

def get_default_config():
    config = HyperscaleConfig()
    config_path = ".hyperscale.config.json"
    if not os.path.exists(config_path):
        with open(config_path, "w") as config_file:
            json.dump(
                config.model_dump(),
                config_file,
                indent=4,
            )

    else:
        with open(config_path, "r") as config_file:
            config_data = json.load(config_file)
            config_data["logs_directory"] = os.path.join(
                os.getcwd(),
                "logs",
            )

            config = HyperscaleConfig(**config_data)

    return config