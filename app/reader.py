

import json
from message import Message
import requests

class MyReader:
  def __init__(self, p_reader, id):
    """A class which wraps the pravega readers

    Arguments:
      * p_reader: A pravega reader instance
      * id: A unique ID
    """
    self.p_reader = p_reader
    self.id       = id

  async def _tell_joke(self):
    f = "https://official-joke-api.appspot.com/random_ten"
    data = requests.get(f)
    tt = json.loads(data.text)[0]
    return tt["setup"] + "\n" + tt["punchline"]

  async def _greet(self, sender):
    return f"{sender} says hi!"

  async def _process_message(self, buff):
    """Processes incoming messages and then decides what 
    to do with them. At the time, the only commands recognized
    are greet and joke. These are akin to slash commands in slack.

    Arguments:
      * buff: A buffer of bytes read from the pravega stream
    
    Returns:
      * resp: A response message to be printed to the console
    """

    # Decode all of the byte strings into regular strings
    decoded   = [x.decode("utf-8") for x in buff]
    stringify = "".join(decoded).strip().lower()
    jsonify   = json.loads(stringify)
    # set to a message so we can ensure it fits the required format
    incoming  = Message(sender=jsonify['sender'], message=jsonify['message'])

    # because we are also listening, we dont want to respond to our own messages
    if (incoming.sender == self.id):
      return
    
    message = jsonify['message']

    # Decide what to do with the commands
    if (message == "greet"):
      msg = await self._greet(incoming.sender)
    elif (message == "joke"):
      msg = await self._tell_joke()
    else:
      msg = message

    resp = Message(sender=incoming.sender, message=msg)
    return resp.dict()

  async def close(self):
    """Close the reader on the pravega stream to avoid dangling
    readers
    """
    self.p_reader.reader_offline()

  async def read(self):
    """Reads next message from the pravega stream. If message
    is non-null, then process it accordingly using helper functions
    """
    try:
        # acquire a segment slice to read
        slice = await self.p_reader.get_segment_slice_async()
        buff  = []
        for event in slice:
          buff.append(event.data())
                
        # after calling release segment, data in this segment slice will not be read again by
        # readers in the same reader group.
        self.p_reader.release_segment(slice)

        # If we got a non-null message, process
        if len(buff) > 0:
          resp = await self._process_message(buff)
          return resp
    except Exception as e:
      logging.error(str(e))