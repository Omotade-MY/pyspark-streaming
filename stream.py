import os
from utility import MyStream, reset_rules, create_socket

BEARER_TOKEN = os.environ['BEARER_TOKEN']


cskt = create_socket()
stream = MyStream(BEARER_TOKEN)
#reset_rules(stream)
stream.start_stream(cskt)