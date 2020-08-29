from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as SocketError

class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message',))


class CommandError(Exception):pass
class Disconnect(Exception):pass


class ProtocolHandler(object):
    def __init__(self):
         handlers = {
            '+':self.handleSimpleString,
            '-':self.handleError,
            ':':self.handleInteger,
            '$':self.handleBinaryString,
            '*':self.handleArray,
            '%':self.handleDictionary,
            '$':self.handleNull
        }
    def handleRequest(self, socket_file):
       pass

    def writeResponse(self, socket_file, data):
       pass

    def handleString(self, socket_file) ->str:
        return socket_file.readline().rstrip('\r\n')

    def handleError(self, socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))

    def handleInteger(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))

    def handleBinaryString(self, socket_file):
        length = int(socket_file.readline().rstrip('\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]
    
    def handleArray(self, socket_file):
        length = int(socket_file.readline().rstrip('\r\n'))
        return [self.handleRequest(socket_file) for i in range(length)]
    
    def handleDict(self, socket_file):
        length = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handleRequest(socket_file) for i in range(length * 2)]
        return dict(zip(elements[::2], elements[1::2]))
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