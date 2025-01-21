from pydantic import BaseModel, StrictInt


class HyperscaleInterfaceConfig(BaseModel):
    update_interval: StrictInt = 5
