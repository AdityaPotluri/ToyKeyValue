from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as SocketError
from Error import CommandError,Disconnect
from ProtocolHandler import ProtocolHandler

Error = namedtuple('Error', ('message',))





class Server(object):
    def __init__(self, host = '127.0.0.1', port = 31337, maxClients = 64):
        self._pool = Pool(maxClients)
        self._server = StreamServer(
            (host,port),
            self.connectionHandler,
            spawn = self._pool
        )
        self._kv = {}
    
    def connectionHandler(self, conn:socket, handler):
        socketFile = conn.makefile('rwb')

        while True:
            try:
                data = self._protocol.handleRequest(socketFile)
            except Disconnect:
                break

            try:
                resp = self.getResponse(data)
            except CommandError as exc:
                resp = Error(exc.args[0])
            
            self._protocol.writeResponse(socketFile,resp)

    def getResponse(self, data):
        pass

    def run(self):
        self._server.serve_forever()