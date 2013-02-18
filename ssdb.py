#!/usr/bin/env python

import socket, os, sys, asynchat, functools, time
from cStringIO import StringIO

DEBUG = True
NL = '\n'


class SSDBConn(asynchat.async_chat):
    debug = DEBUG
    result = []
    def collect_incoming_data(self, data):
        self.incoming.append(data)

    def found_terminator(self):
        if isinstance(self.get_terminator(), int):
            self.result.append(self._get_data())

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
        # self.sock.setblocking(0)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # time.sleep(1)
        self.sock.connect(tuple(self.addr))
        # self.conn = SSDBConn(self.sock)

        commands = ['get', 'set', 'del', 'incr', 'decr', 'keys', 'scan', 'rscan', 
        'multi_get', 'multi_set', 'multi_del', 
        # hash
        'hdecr', 'hdel', 'hget', 'hincr', 'hkeys', 'hlist', 'hrscan', 'hscan', 'hset', 'hsize', 
        # sorted list
        'zdecr', 'zdel', 'zget', 'zincr', 'zkeys', 'zlist', 'zrscan', 'zscan', 'zset', 'zsize']
        for cmd in commands:
            setattr(self, cmd, functools.partial(self.operation, cmd, ))

    def operation(self, cmd, *args):
        sbuffer = [cmd]
        sbuffer.extend(args)
        sbuffer = NL.join(map(lambda x:'%d%s%s' % (len(x), NL, x), sbuffer)) + NL*2
        if DEBUG: print sbuffer
        bytes = self.sock.send(sbuffer)
        if DEBUG: print '%s: %sBytes' % (cmd, bytes)

        incoming = bytearray() # the reading buffer
        results = [] # outcome results
        pos = 0 # current reading cursor position relative to incoming
        tlen = 0 # total data read

        term_pos = True # assert there will be a term from the start

        eof  = False # EOF not found
        term = NL # either str NL or length

        while not eof:
            print 'loop, %r, %r, %r, %r' % (term, pos, term_pos, results)
            if pos>tlen or pos==tlen==0:
                data = self.sock.recv(self.RECV_BUFFER_SIZE)
                incoming.extend(data)
                tlen += len(data)
            if isinstance(term, str):
                term_pos = incoming.find(term, pos)
                if term_pos==-1:
                    if pos:
                        incoming = bytearray(incoming[pos:])
                    continue
                elif term_pos==0:
                    eof = True
                    break
                elif term_pos>0:
                    print 'loop, %r, %r, %r, %r' % (term, pos, term_pos, results)
                    print repr(incoming)
                    next_term = int(incoming[pos:term_pos]) # length of the value field
                    pos += term_pos+len(term)
                    term = next_term
            if isinstance(term, int):
                if pos+term>tlen:
                    print pos, term, tlen
                    data = self.sock.recv(self.RECV_BUFFER_SIZE)
                    incoming.extend(data)
                    tlen += len(data)
                    continue
                else:
                    results.append(incoming[pos:pos+term])
                    pos += term + len(NL)
                    term = NL
        return results





if __name__ == '__main__':
    import readline, rlcompleter; readline.parse_and_bind("tab: complete")
    db = SSDB()
    # time.sleep(1)
    db.incr('a')
