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
REQ_PREFETCH = 19
REQ_FLUSH = 20
REQ_RELEASE = 21
REQ_FGETATTR = 22
REQ_FTRUNCATE = 23

# requests related to Mogami's system
REQ_CLOSE = 24
REQ_DATAADD = 25
REQ_DATADEL = 26
REQ_RAMFILEADD = 27
REQ_RAMFILEDEL = 28
REQ_FILEDEL = 29
REQ_FILEASK = 30

# requests related to scheduler
REQ_ADDAP = 31
REQ_SCHEDULE = 32

# channel's type
TYPE_TCP = 0
TYPE_UNIX = 1

class MogamiChannelRepository(object):
    """Class for repository of all channels.

    This class is not used now!
    """
    def __init__(self, ):
        self.channel_dict = {}
        self.lock = threading.Lock()
        
    def get_channel(self, dest):
        with self.lock:
            if dest not in self.channel_dict:
                return None
            else:
                return self.channel_dict[dest]

    def set_channel(self, dest, d_channel, p_channel):
        with self.lock:
            self.channel_dict[dest] = (d_channel, p_channel)

class MogamiChannel(object):
    """Class for communication with others using TCP
    """    
    def __init__(self, mode=TYPE_TCP):
        """initializer of mogami's channel
        default mode is tcp socket mode 

        @param mode socket type used in this class (optional)
        """
        if mode == TYPE_UNIX:
            self.connect = self.unix_connect

        # lock for the socket
        self.lock = threading.Lock()
        self.peername = None
        self.myname = None

    def unix_mk_listening_socket(self, path):
        """
        """
        # make a socket to communicate with others
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(path)
        self.sock.listen(10)

    def unix_connect(self, path):
        """This function might be replaced to connect,
        if the type of self_channel is TYPE_UNIX.

        @param path path of named pipe to connect
        """
        # make a socket to communicate with others
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(path)

    def connect(self, dist, port):
        """connect to specified server

        @param dist distination IP or hostname to connect
        @param port port number to connect
        """
        # make a socket to communicate with others
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((dist, port))

        # set name (IP address)
        self.peername = self.sock.getpeername()[0]
        self.myname = self.sock.getsockname()[0]

    def set_socket(self, sock):
        """create a channel from a socket

        @param sock socket object that has already been count to another socket
        """
        self.sock = sock

        # set name (IP address)
        peername = self.sock.getpeername()
        if len(peername) > 0:
            self.peername = peername[0]
        myname = self.sock.getsockname()
        if len(myname) > 0:
            self.myname = myname[0]

    def getmyname(self, ):
        """return name (IP address) of myself.
        """
        return self.myname
    
    def getpeername(self, ):
        """return name (IP address) of connected peer.
        """
        return self.peername

    def sendall(self, data):
        """send all data.

        This function may return errno, when send error occurs.
        @param data data to send
        """
        try:
            self.sock.sendall(data)
        except Exception, e:
            MogamiLog.debug("** Error in sending data **")
            return None
        return len(data)

    def recvall(self, length):
        """recv all data.

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
        """send packed message.

        @param data data to send
        """
        buf = cPickle.dumps(data)
        while conf.bufsize - 3 < len(buf):
            ret = self.sendall(buf[:conf.bufsize - 3] + "---")
            if ret == None:
                MogamiLog.error("send_msg error")
                break
            buf = buf[conf.bufsize - 3:]
        buf = buf + "-" * (conf.bufsize - len(buf) - 3) + "end"
        self.sendall(buf)

    def recv_msg(self, ):
        """receive packed message.
        """
        res_buf = cStringIO.StringIO()
        buf = ""
        while buf[-3:] != "end":
            buf = self.recvall(conf.bufsize)
            if len(buf) != conf.bufsize:
                MogamiLog.error("recv_msg error")
                return None
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
        """finalize mogami channel.
        """
        self.sock.close()


class MogamiChanneltoMeta(MogamiChannel):
    def __init__(self, dest=None):
        MogamiChannel.__init__(self)
        if dest != None:
            MogamiChannel.connect(self, dest, conf.metaport)

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

    def symlink_req(self, frompath, topath):
        with self.lock:
            self.send_msg((REQ_SYMLINK, frompath, topath))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def readlink_req(self, path):
        with self.lock:
            self.send_msg((REQ_READLINK, path))
            ans = self.recv_msg()
        # (0 or errno, result)
        return ans

    def truncate_req(self, path, length):
        with self.lock:
            self.send_msg((REQ_TRUNCATE, path, length))
            ans = self.recv_msg()
        # (0 or errno, dest, data_path)
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
        # (0 or errno, dest, metafd, datapath, size, created)
        return ans

    def release_req(self, metafd, fsize):
        with self.lock:
            self.send_msg((REQ_RELEASE, metafd, fsize))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def dataadd_req(self, rootpath):
        with self.lock:
            self.send_msg((REQ_DATAADD, rootpath))

    def datadel_req(self, ):
        with self.lock:
            self.send_msg((REQ_DATADEL, ))


class MogamiChanneltoData(MogamiChannel):
    def __init__(self, dest=None):
        MogamiChannel.__init__(self)
        if dest != None:
            MogamiChannel.connect(self, dest, conf.dataport)

    def connect(self, data_ip):
        """connect to data server.

        @param data_ip ip address of data server to connect
        """
        MogamiChannel.connect(self, data_ip, conf.dataport)

    def open_req(self, datapath, flags, *mode):
        with self.lock:
            self.send_msg((REQ_OPEN, datapath, flags, mode))
            ans = self.recv_msg()
        # (0 or errno, datafd, time to open)
        return ans

    def read_req(self, datafd, blnum):
        with self.lock:
            self.send_msg((REQ_READ, datafd, blnum))
            header = self.recv_msg()
            if header == None:
                return None

            errno = header[0]
            blnum = header[1]
            size = header[2]

            if size == 0:
                return ""

            buf = self.recvall(size)
            
        return buf


    def prefetch_req(self, datafd, blnum_list):
        with self.lock:
            self.send_msg((REQ_PREFETCH, datafd, blnum_list))

    def flush_req(self, datafd, w_list, w_data):
        w_list_pickled = cPickle.dumps(w_list)
        with self.lock:
            self.send_msg((REQ_FLUSH, datafd, len(w_list_pickled), len(w_data)))
            self.sendall(w_list_pickled)
            self.sendall(w_data)
            ans = self.recv_msg()
        return ans

    def recv_bldata(self, ):
        pass

    def truncate_req(self, path, length):
        with self.lock:
            self.send_msg((REQ_TRUNCATE, path, length))
            ans = self.recv_msg()
        # 0 or errno
        return ans

    def release_req(self, datafd):
        with self.lock:
            self.send_msg((REQ_RELEASE, datafd))
            ans = self.recv_msg()
        # (0 or errno, fsize)
        return ans

    def close_req(self, ):
        with self.lock:
            self.send_msg((REQ_CLOSE, ))

    def delfile_req(self, files):
        with self.lock:
            self.send_msg((REQ_FILEDEL, files))
            ans = self.recv_msg()
        return ans


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

    def symlink_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def readlink_answer(self, ans, result):
        with self.lock:
            self.send_msg((ans, result))

    def truncate_answer(self, ans, dest, data_path):
        with self.lock:
            self.send_msg((ans, dest, data_path))

    def utime_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def open_answer(self, ans, dest, fd, data_path, size, created):
        with self.lock:
            self.send_msg((ans, dest, fd, data_path, size, created))

    def release_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def fileask_answer(self, dest_dict):
        with self.lock:
            self.send_msg(dest_dict)

class MogamiChannelforData(MogamiChannelforServer):
    def truncate_answer(self, ans):
        with self.lock:
            self.send_msg(ans)

    def open_answer(self, ans, datafd, t_time):
        with self.lock:
            self.send_msg((ans, datafd, t_time))

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
            self.send_msg((ans, size))

    def filedel_answer(self, ans):
        with self.lock:
            self.send_msg(ans)


class MogamiChanneltoTellAP(MogamiChannel):
    def __init__(self, pipepath):
        MogamiChannel.__init__(self, TYPE_UNIX)
        MogamiChannel.unix_mk_listening_socket(self, pipepath)

    def accept_with_timeout(self, timeout):
        self.sock.settimeout(timeout)
        try:
            (sock, address) = self.sock.accept()
        except socket.timeout:
            return None
        c_channel = MogamiChannel(TYPE_UNIX)
        c_channel.set_socket(sock)

        # return channel of client
        return c_channel

class MogamiChanneltoAskAP(MogamiChannel):
    def __init__(self, pipepath):
        MogamiChannel.__init__(self, TYPE_UNIX)
        self.connect(pipepath)

    def file_access_req(self, pid):
        self.send_msg(pid)
        file_access_list = self.recv_msg()
        # this file access list if might be empty
        return file_access_list
