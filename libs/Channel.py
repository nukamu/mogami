#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import sys
import os
import threading
import socket
import select
import time
import string
import cStringIO
import cPickle

from System import MogamiLog
sys.path.append(os.pardir)
from conf import conf

# requests related to metadata
REQ_GETATTR = 0
REQ_READDIR = 1
REQ_ACCESS = 2
REQ_MKDIR = 3
REQ_RMDIR = 4
REQ_UNLINK = 5
REQ_RENAME = 6
REQ_MKNOD = 7
REQ_CHMOD = 8
REQ_CHOWN = 9
REQ_LINK = 10
REQ_SYMLINK = 11
REQ_READLINK = 12
REQ_TRUNCATE = 13
REQ_UTIME = 14
REQ_FSYNC = 15

# requests related to files
REQ_OPEN = 16
REQ_CREATE = 17
REQ_READ = 18
REQ_FLUSH = 19
REQ_RELEASE = 20
REQ_FGETATTR = 21
REQ_FTRUNCATE = 22

# requests related to Mogami's system
REQ_CLOSE = 23
REQ_DATAADD = 24
REQ_DATADEL = 25
REQ_RAMFILEADD = 26
REQ_RAMFILEDEL = 27
REQ_FILEDEL = 28
REQ_FILEASK = 29

# requests related to scheduler
REQ_ADDAP = 30
REQ_SCHEDULE = 31

# Channel's type
TYPE_TCP = 0
TYPE_UNIX = 1


class MogamiChannel(object):
    """the class for communication with others using TCP
    """    
    def __init__(self, *mode):
        """initializer of mogami's channel
        default mode is tcp socket mode 

        @param *mode socket type used in this class (optional)
        """
        # make a socket to communicate with others
        if len(mode) == 0:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        elif mode[0] == TYPE_TCP:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        elif mode[0] == TYPE_UNIX:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.connect = self.unix_connect

        # lock for the socket
        self.lock = threading.Lock()

        self.peername = None

    def unix_connect(self, path):
        """This function might be replaced to connect,
        if the type of self_channel is TYPE_UNIX.

        @param path path of named pipe to connect
        """
        self.sock.connect(path)

    def connect(self, dist, port):
        """connect to specified server

        @param dist distination IP or hostname to connect
        @param port port number to connect
        """
        self.sock.connect((dist, port))

    def set_socket(self, sock):
        """create a channel from a socket

        @param sock socket object that has already been count to another socket
        """
        self.sock = sock

    def getpeername(self, ):
        if self.peername == None:
            self.peername = self.sock.getpeername()[0]
        return self.peername

    def sendall(self, data):
        """send all data

        This function may return errno, when send error occurs.
        @param data data to send
        """
        try:
            self.sock.sendall(data)
        except Exception, e:
            MogamiLog.debug("** Error in sending data **")
            return e.errno
        return len(data)

    def recvall(self, length):
        """recv all data

        This function may return less data than required.
        (when connection closed)
        @param length length of data to receive
        """
        buf = cStringIO.StringIO()
        recvlen = 0
        while recvlen < length:
            try:
                recvdata = self.sock.recv(length - recvlen)
            except Exception, e:
                MogamiLog.error("** Connection closed suddenly **")
                self.sock.close()
                break
            if recvdata == "":
                self.sock.close()
                break
            buf.write(recvdata)
            recvlen += len(recvdata)
        data = buf.getvalue()
        return data

    def send_msg(self, data):
        """
        """
        buf = cPickle.dumps(data)
        while conf.bufsize - 3 < len(buf):
            ret = self.sendall(buf[:conf.bufsize - 3] + "---")
            if ret == None:
                break
            buf = buf[conf.bufsize - 3:]
        buf = buf + "-" * (conf.bufsize - len(buf) - 3) + "end"
        self.sendall(buf)

    def recv_msg(self, ):
        """
        """
        res_buf = cStringIO.StringIO()
        buf = self.recvall(conf.bufsize)
        if len(buf) != conf.bufsize:
            return None
        res_buf.write(buf[:-3])
        while buf[-3:] != "end":
            buf = self.recvall(conf.bufsize)
            res_buf.write(buf[:-3])
        ret = cPickle.loads(res_buf.getvalue())
        return ret

    def recvall_with_time(self, length):
        """receive data and measure time to receive

        This function seems to be called only in prefetching.
        @param length length of data to receive
        """
        buf = cStringIO.StringIO()
        recvlen = 0
        recv_time = 0
        while recvlen < length:
            try:
                start_t = time.time()
                recvdata = self.sock.recv(length - recvlen)
                end_t = time.time()
            except Exception, e:
                MogamiLog.error("** Connection closed suddenly **")
                self.sock.close()
                break
            if recvdata == "":
                self.sock.close()
                break
            buf.write(recvdata)
            recvlen += len(recvdata)
            recv_time += end_t - start_t
        return (buf.getvalue(), recv_time)

    def finalize(self, ):
        """finalizer of mogami channel.
        """
        self.sock.close()


class MogamiChanneltoMeta(MogamiChannel):
    """
    """
    def __init__(self, *dest):
        """
        """
        MogamiChannel.__init__(self)
        if len(dest) > 0:
            self.connect(dest[0], conf.metaport)

    def connect(self, meta_ip):
        MogamiChannel.connect(self, meta_ip, conf.metaport)

    def getattr_req(self, path):
        with self.lock:
            self.send_msg((REQ_GETATTR, path))
            ans = self.recv_msg()
        # (0 or errno, st, fsize)
        return ans

    def readdir_req(self, path, offset):
        with self.lock:
            self.send_msg((REQ_READDIR, path, offset))
            ans = self.recv_msg()
        # (0 or errno, list_of_contents)
        return ans

    def access_req(self, path, mode):
        with self.lock:
            self.send_msg((REQ_ACCESS, path, mode))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def mkdir_req(self, path, mode):
        with self.lock:
            self.send_msg((REQ_MKDIR, path, mode))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def rmdir_req(self, path):
        with self.lock:
            self.send_msg((REQ_RMDIR, path))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def unlink_req(self, path):
        with self.lock:
            self.send_msg((REQ_UNLINK, path))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def rename_req(self, oldpath, newpath):
        with self.lock:
            self.send_msg((REQ_RENAME, oldpath, newpath))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def chmod_req(self, path, mode):
        with self.lock:
            self.send_msg((REQ_CHMOD, path, mode))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def chown_req(self, path, uid, gid):
        with self.lock:
            self.send_msg((REQ_CHOWN, path, uid, gid))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def truncate_req(self, path, length):
        with self.lock:
            self.send_msg((REQ_TRUNCATE, path, length))
            ans = self.recv_msg()
        # (0 or errno, dest, filaname)
        return ans

    def utime_req(self, path, times):
        with self.lock:
            self.send_msg((REQ_UTIME, path, times))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def open_req(self, path, flags, *mode):
        with self.lock:
            self.send_msg((REQ_OPEN, path, flags, mode))
            ans = self.recv_msg()
        # (0 or errno, dist, metafd, datapath, size, created)
        return ans

    def dataadd_req(self, rootpath):
        with self.lock:
            self.send_msg((REQ_DATAADD, rootpath))

    def datadel_req(self, ):
        with self.lock:
            self.send_msg((REQ_DATADEL, ))


class MogamiChanneltoData(MogamiChannel):
    """
    """
    def __init__(self, *con_info):
        MogamiChannel.__init__(self)
        if len(con_info) > 0:
            self.connect(con_info[0], conf.dataport)

    def open_req(self, datapath, created, flags, *mode):
        with self.lock:
            self.send_msg((REQ_OPEN, datapath, created, flags, mode))
            ans = self.recv_msg()
        # (0 or errno, time to open)
        return ans

    def read_req(self, datafd, blnum):
        with self.lock:
            self.send_msg((REQ_READ, datafd, blnum))
            ans = self.recv_msg()
            # ans = (0 or errno, blnum, size)
            bldata = self.recvall(size)
        return (ans, bldata)

    def prefetch_req(self, datafd, blnum_list):
        with self.lock:
            self.send_msg((REQ_PREFETCH, datafd, blnum_list))

    def flush_req(self, datafd):
        with self.lock:
            self.send_msg((REQ_FLUSH, datafd, ))


class MogamiChannelforServer(MogamiChannel):

    def recv_request(self, ):
        return self.recv_msg()


class MogamiChannelforMeta(MogamiChannelforServer):

    def getattr_answer(self, ans, st, fsize):
        with self.lock:
            self.send_msg((ans, st, fsize))

    def readdir_answer(self, ans, dir_list):
        with self.lock:
            self.send_msg((ans, dir_list))

    def access_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def mkdir_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def rmdir_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def unlink_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def rename_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def chmod_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def chown_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def truncate_answer(self, ans, dest, data_path):
        with self.lock:
            self.send_msg((ans, dest, data_path))

    def utime_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def open_answer(self, ans, dest, metafd, size, data_path, created):
        with self.lock:
            self.send_msg((ans, dest, metafd, size, data_path, created))

    def release_answer(self, ans):
        with self.lock:
            self.send_msg(ans)


class MogamiChannelforData(MogamiChannelforServer):
    def truncate_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def open_answer(self, ans, t_time):
        with self.lock:
            self.send_msg((ans, t_time))

    def data_send(self, ans, blnum, size, data):
        with self.lock:
            self.send_msg((ans, blnum, size))
            if size != 0:
                self.sendall(data)

    def flush_recv_data(self, listlen, datalen):
        with self.lock:
            buf = self.recvall(listlen)
            write_list = cPickle.loads(buf)
            buf = self.recvall(datalen)
        return (write_list, buf)

    def flush_answer(self, ans, write_len):
        with self.lock:
            self.send_msg((ans, write_len))

    def release_answer(self, ans, size):
        with self.lock:
            self.send_msg(ans, size)

    def filedel_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

class MogamiChannelforScheduler(MogamiChannel):
    def __init__(self, path):
        MogamiChannel.__init__(self)


class MogamiChanneltoScheduler(MogamiChannel):
    def __init__(self, ):
        pass
