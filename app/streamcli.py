"""streamcli

Usage:
  streamcli.py <url> <id> [--verbose]
  streamcli.py --version

Options:
  -h --help                          Show this screen.
  --version                          Show version.
  --verbose                          High Verbosity.

Example:
  streamcli.py tcp://127.0.0.1:9090 tim --verbose 
  streamcli.py tcp://127.0.0.1:9090 alex
"""

import pravega_client
import asyncio
from docopt import docopt
import logging 
import json
import requests
from reader import MyReader
from writer import MyWriter
from chatter import MyChatter
import signal

async def main():
  # Pass our docstring to docopt for parsing
  arguments = docopt(__doc__, version='streamcli 0.1.0')

  log_level = logging.DEBUG if arguments['--verbose'] else logging.INFO

  # set log level based on --verbose flag
  logging.basicConfig(level=log_level)

  # Pull data out of the user passed arguments
  url     = arguments['<url>']
  chat_id = arguments['<id>']

  # Create a stream manager, scope, and stream
  # for the app to use. Will be created each time
  # an app instance is run, but doesnt affect pravega
  stream_manager = pravega_client.StreamManager(url)
  stream_manager.create_scope("scope")
  stream_manager.create_stream("scope", "stream", 2)
  reader_group = stream_manager.create_reader_group(chat_id, "scope", "stream")

  # Create pravega readers, writers, and our wrappers
  reader       = reader_group.create_reader(chat_id)
  my_reader    = MyReader(reader, chat_id)
  writer       = stream_manager.create_writer("scope","stream")
  my_writer    = MyWriter(writer, chat_id)

  # Create chat instance with the reader and writer wrappers above
  chatter = MyChatter(my_reader, my_writer, chat_id)

  # Create scoped function which will be invoked on CTRL+C to
  # gracefully close readers / chatters
  def signal_handler(*args):
    chatter.close()

  signal.signal(signal.SIGINT, signal_handler)

  # Run the chat
  await chatter.chat()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
  loop.close()