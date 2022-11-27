from pydantic import BaseModel

class Message(BaseModel):
    sender: str
    message: str