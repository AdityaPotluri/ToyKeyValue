
from collections import namedtuple
from io import BytesIO
from socket import error as SocketError
from Error import CommandError, Disconnect










class ProtocolHandler(object):
    Error = namedtuple('Error', ('message',))
    
    def __init__(self):
        self.handlers = {
            '+':self.handleSimpleString,
            '-':self.handleError,
            ':':self.handleInteger,
            '$':self.handleBinaryString,
            '*':self.handleArray,
            '%':self.handleDictionary,
            '$':self.handleNull
        }
    def writeResponse(self, socket_file, data):
        buf = BytesIO()
        self._write(buf,data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf ,data):
        #TODO convert errything to F strings to clean up
        if isinstance(data,str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write( f'${len(data)}\r\n{data}\r\n' )
            
        elif isinstance(data, int):
            buf.write(f':{data}\r\n')
            buf.write(':%s\r\n' % data)
        elif isinstance(data, Error):
            buf.write('-%s\r\n' % Error.message)
        elif isinstance(data, (list, tuple)):
            buf.write('*%s\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write('%%%s\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write('$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s' % type(data))

    def handleRequest(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    

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