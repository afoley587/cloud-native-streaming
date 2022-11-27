import asyncio
from reader import MyReader
from writer import MyWriter
from threading import Thread 
import logging 

class MyChatter:
  def __init__(self, reader, writer, id):
    """A class which has a pravega writer, a pravega reader, 
    and an id

    Arguments:
      * reader: A pravega reader instance
      * writer: A pravega writer instance
      * id: A unique ID
    """
    self.reader  = reader
    self.writer  = writer
    self.running = True
    self.id      = id

  async def chat(self):
    """Begins the chat read/write threads
    """
    reader_t = Thread(target=asyncio.run, args=(self.read(),))
    writer_t = Thread(target=self.write)
    reader_t.start()
    writer_t.run()

  async def read(self):
    """Continuously listens for new messages from the pravega
    broker
    """
    while(self.running): # Will be set to false when close is called
      msg = await self.reader.read()
      if (msg):
        logging.debug(msg)
        # Backspaces to make it look like the input prompt stays at the 
        # bottom of the screen for easier reading
        print('\b' * (len(self.id) + 2), end="", flush=True)
        # Print the message, and then re-enter the prompt line
        print(f"{msg['sender']}: {msg['message']}\n")
        print(f"{self.id}: ", end="", flush=True)
    # Close the reader after this finishes and the chatter
    # requests a closed connection
    await self.reader.close()
  
  def write(self):
    """Continuously listens for new messages from the user to then
    write to the pravega broker
    """
    while(self.running): # Will be set to false when close is called
      print("")
      message = input(f"{self.id}: ").strip().lower()
      if (message):
        self.writer.write(message)

  def close(self):
    """Tell threads to close themselves and exit gracefully
    """
    self.running = False

