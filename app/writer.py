import json
from message import Message

class MyWriter:
  def __init__(self, p_writer, id):
    """A class which wraps the pravega writers

    Arguments:
      * p_writer: A pravega writer instance
      * id: A unique ID
    """
    self.p_writer = p_writer
    self.id       = id

  def _format_message(self, message):
    """Format the message to the Message format defined
    for our system

    Arguments:
      * message: Text content of the message in question

    Returns:
      * json: Json representation string to be put on the pravega stream
    """
    msg = Message(sender=self.id, message=message)
    return msg.json()

  def write(self, message):
    """Writes a message to the pravega stream

    Arguments:
      * message: The message to write to the stream
    """
    msg = self._format_message(message)
    self.p_writer.write_event(msg)
