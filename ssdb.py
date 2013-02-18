#!/usr/bin/env python

import socket, os, sys, asynchat, functools, time
from cStringIO import StringIO

DEBUG = True
NL = '\n'

def dbg(*x):
    if DEBUG:
        print ' '.join(x)


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
        sbuffer = NL.join(map(lambda x:'%d%s%s' % (len(str(x)), NL, x), sbuffer)) + NL*2
        bytes = self.sock.send(sbuffer)
        dbg('command `%s` sent %sBytes' % (cmd, bytes))

        incoming = bytearray() # the reading buffer
        results = [] # outcome results

        spos = 0 # start position relative to incoming
        epos = 0 # end position   relative to incoming

        RNL = NL # response new line
        term = RNL # either str NL or length
        need_more = True

        while True:
            if need_more:
                incoming.extend(self.sock.recv(self.RECV_BUFFER_SIZE))
            dbg('loop', repr(str(incoming)))
            if isinstance(term, str):
                epos = incoming.find(term, spos)
                if epos == -1:
                    need_more = True
                    continue
                elif epos == len(incoming)-1 :
                    break
                elif epos > 0:
                    dbg( 'try length\t%r[%s:%s] by %r' % (str(incoming), spos, epos, term))
                    term = incoming[spos:epos]
                    dbg( 'got length\t%r' % (str(term)))
                    spos = epos + len(RNL)
                    term = int(term)
            if isinstance(term, int):
                epos = spos + term
                dbg('try value\t%r[%s:%s] by %r' % (str(incoming), spos, epos, term))
                if epos>len(incoming):
                    need_more = True
                    continue
                else:
                    data = incoming[spos:epos]
                    results.append(str(data))
                    term = RNL
                    spos = epos + len(term)
                    dbg( 'got value\t%r' % (results))
                    need_more = False
        if results[0] == 'ok':
            r = results[1:]
            return r[0] if len(r)==1 else r
        else:
            raise Exception(results[0])





if __name__ == '__main__':
    import readline, rlcompleter; readline.parse_and_bind("tab: complete")
    db = SSDB()
    # time.sleep(1)
    db.incr('a')
