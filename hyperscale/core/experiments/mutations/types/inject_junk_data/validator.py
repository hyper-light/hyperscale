from pydantic import BaseModel, StrictInt


class InjectJunkDataValidator(BaseModel):
    junk_size: StrictInt
