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
        self._commands = self.getCommands() 
    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1
    
    def delete(self, key, value):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0
    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]
    
    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key,value in data:
            self._kv[key] = value
        return len(data)
    def getCommands(self):
        return {
            'GET':self.get,
            'SET':self.set,
            'DELETE':self.delete,
            'FLUSH':self.flush,
            'MGET':self.mget,
            'MSET':self.mset
        }
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
        if not isinstance(data,list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string')
        if not data:
            raise CommandError('Missing Command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(f'Unrecognized command {command}')

        return self._commands[command](*data[1:])
    
    def run(self):
        self._server.serve_forever()