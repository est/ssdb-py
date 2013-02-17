#!/usr/bin/env python

import socket, os, sys, asynchat, functools
from cStringIO import StringIO

DEBUG = True
NL = '\n'


class SSDBConn(asynchat.async_chat):
    debug = DEBUG
    result = []
    def collect_incoming_data(self, data):
        self.incoming.append(data)

    def found_terminator(self):
        if isinstance(self.terminator, int):
            result.append(self._get_data())

class SSDB(object):
    addr = None
    RECV_BUFFER_SIZE = 4096
    SEND_BUFFER_SIZE = 4096
    def __init__(self, *args, **kwargs):
        """using either:
         - ssdb://
         - ip:port
         - host='', port=''
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if len(args)==1:
            t = args[0].partition('//'
                )[2].rpartition('@'
                )[2].rpartition(':')
            self.addr = (
                t[0].replace('[', '').replace(']', ''), 
                t[2]
            )
        elif len(args) == 2:
            self.addr = args
        h, p = kwargs.get('host'), kwargs.get('port')
        # handle default server address here
        t = list(self.addr or ['127.0.0.1', 8888]) 
        if h:
            t[0] = h
        if p:
            t[1] = p
        self.addr = t
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect(tuple(self.addr))
        self.conn = SSDBConn(self.sock)

        commands = ['get', 'set', 'del', 'incr', 'decr', 'keys', 'scan', 'rscan', 
        'multi_get', 'multi_set', 'multi_del', 
        # hash
        'hdecr', 'hdel', 'hget', 'hincr', 'hkeys', 'hlist', 'hrscan', 'hscan', 'hset', 'hsize', 
        # sorted list
        'zdecr', 'zdel', 'zget', 'zincr', 'zkeys', 'zlist', 'zrscan', 'zscan', 'zset', 'zsize']
        for cmd in commands:
            setattr(self, cmd, functools.partial(self.operation, cmd, ))

    def parse(self, rbuffer):
        result = []
        cursor = 0
        all_length = len(rbuffer)
        while cursor < all_length:
            c = rbuffer.find(NL)
            if c==-1:
                break
            else:
                x, y = cursor, c
                r = rbuffer[cursor:c]
                cursor = y + len(NL)
                result.append(rbuffer[cursor:r])
        return result

    def operation(self, cmd, *args):
        sbuffer = [cmd]
        sbuffer.extend(args)
        sbuffer = NL.join(map(lambda x:'%d%s%s' % (len(x), NL, x), sbuffer)) + NL*2
        if DEBUG: print sbuffer
        bytes = self.sock.send(sbuffer)
        if DEBUG: print '%s: %sBytes' % (cmd, bytes)

        loop = True
        chunks = []
        results = []
        cursor = (0, 0)

        while loop:
            chunks.append(self.sock.recv(self.RECV_BUFFER_SIZE))
            # if len(s) < self.RECV_BUFFER_SIZE:
            #     continue
            # else:
            c = chunks[cursor[0]].find()

        print repr(rtn)
        loop = True
        result = []
        cursor = 0
        while loop:
            l, cursor = xslice(rtn, cursor, NL)



if __name__ == '__main__':
    import readline, rlcompleter; readline.parse_and_bind("tab: complete")
    db = SSDB()
