from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from ProtocolHandler import ProtocolHandler
from Error import CommandError
from collections import namedtuple

class Client(object):
    Error = namedtuple('Error', ('message',))

    def __init__(self, host = '127.0.0.1', port = 31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_NET, socket.SOCK_STREAM )
        self._socket.connect((host,port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.writeResponse(self._fh, *args)
        resp = self._protocol.handleRequest(self._fh)

        if isinstance(resp, Error):
            raise CommandError(resp.message)
        
        return resp
        
    def get(self, key):
        return self.execute()