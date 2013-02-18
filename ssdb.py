#!/usr/bin/env python

import socket, functools



class SSDB(object):
    DEBUG = False
    RECV_BUFFER_SIZE = 4096
    SEND_BUFFER_SIZE = 4096
    NEWLINE = '\n'
    def __init__(self, host='127.0.0.1', port=8888):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((host, port))

        commands = ['get', 'set', 'del', 'incr', 'decr', 'keys', 'scan', 'rscan', 
        'multi_get', 'multi_set', 'multi_del', 
        # hash
        'hdecr', 'hdel', 'hget', 'hincr', 'hkeys', 'hlist', 'hrscan', 'hscan', 'hset', 'hsize', 
        # sorted list
        'zdecr', 'zdel', 'zget', 'zincr', 'zkeys', 'zlist', 'zrscan', 'zscan', 'zset', 'zsize']
        for cmd in commands:
            setattr(self, cmd, functools.partial(self.operation, cmd, ))

    def dbg(self, *x):
        if self.DEBUG:
            print ' '.join(x)

    def operation(self, cmd, *args):
        sbuffer = [cmd]
        sbuffer.extend(args)
        sbuffer = self.NEWLINE.join( map(
            lambda x:'%d%s%s' % (len(str(x)), 
            self.NEWLINE, x), sbuffer
        )) + self.NEWLINE*2
        bytes = self.sock.send(sbuffer)
        self.dbg('command `%s` sent %sBytes' % (cmd, bytes))

        incoming = bytearray() # the reading buffer
        results = [] # outcome results

        spos = 0 # start position relative to incoming
        epos = 0 # end position   relative to incoming

        RNL = '\n' # response new line
        term = RNL # either str NL or length
        need_more = True

        while True:
            if need_more:
                incoming.extend(self.sock.recv(self.RECV_BUFFER_SIZE))
            self.dbg('loop', repr(str(incoming)))
            if isinstance(term, str):
                epos = incoming.find(term, spos)
                if epos == -1:
                    need_more = True
                    continue
                elif epos == len(incoming)-1 :
                    break
                elif epos > 0:
                    self.dbg( 'try length\t%r[%s:%s] by %r' % (str(incoming), spos, epos, term))
                    term = incoming[spos:epos]
                    self.dbg( 'got length\t%r' % (str(term)))
                    spos = epos + len(RNL)
                    term = int(term)
            if isinstance(term, int):
                epos = spos + term
                self.dbg('try value\t%r[%s:%s] by %r' % (str(incoming), spos, epos, term))
                if epos>len(incoming):
                    need_more = True
                    continue
                else:
                    data = str(incoming[spos:epos])
                    if 'desc' in cmd or 'incr' in cmd and data.lower()!='ok':
                        data = int(data)
                    results.append(data)
                    term = RNL
                    spos = epos + len(term)
                    self.dbg( 'got value\t%r' % (results))
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
