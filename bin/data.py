#! /usr/bin/env python
#-*- coding: utf-8 -*-

import os
import os.path
import sys
import errno
import string
import random
import threading
import re
import time
import stat
import socket
import cPickle
import cStringIO

import atexit    # for leave from meta data sever list

sys.path.append(os.pardir)

from libs import Channel
from libs import System
from libs.System import MogamiLog
from conf import conf


class MogamiDataHandler(System.MogamiDaemons):
    """This is the class for thread created for each client.
    This handler is run as multithread.
    """
    def __init__(self, c_channel, rootpath):
        System.MogamiDaemons.__init__(self)
        self.c_channel = c_channel
        self.rootpath = rootpath

    def run(self, ):
        while True:
            req = self.c_channel.recv_request()
            if req == None:
                MogamiLog.debug("Connection closed")
                self.c_channel.finalize()
                break

            if req[0] == Channel.REQ_OPEN:
                MogamiLog.debug('** open **')
                self.open(req[1], req[2], *req[3])

            elif req[0] == Channel.REQ_CREATE:
                MogamiLog.debug('** create **')
                self.create(req[1], req[2], req[3])

            elif req[0] == Channel.REQ_READ:
                MogamiLog.debug('** read **')
                self.read(req[1], req[2])

            elif req[0] == Channel.REQ_PREFETCH:
                MogamiLog.debug('** prefetch')
                self.prefetch(req[1], req[2])

            elif req[0] == Channel.REQ_FLUSH:
                MogamiLog.debug('** flush')
                self.flush(req[1], req[2], req[3])

            elif req[0] == Channel.REQ_RELEASE:
                MogamiLog.debug('** release')
                self.release(req[1])
                break

            elif req[0] == Channel.REQ_TRUNCATE:
                MogamiLog.debug('** truncate')
                self.truncate(req[1], req[2])

            elif req[0] == Channel.REQ_FTRUNCATE:
                MogamiLog.debug('** ftruncate')

            elif req[0] == Channel.REQ_CLOSE:
                MogamiLog.debug("** quit **")
                self.c_channel.finalize()
                break

            elif req[0] == Channel.REQ_FILEDEL:
                MogamiLog.debug("** filedel")
                self.filedel(req[1])

            else:
                MogamiLog.debug('** this is unexpected header. break!')
                break

    def truncate(self, path, length):
        MogamiLog.debug("path = %s. length = %d" % (path, length))
        try:
            f = open(path, "r+")
            f.truncate(length)
            f.close()
            ans = 0
        except Exception, e:
            MogamiLog.error("have an error (%s)" % (e))
            ans = e.errno
        self.c_channel.truncate_answer(ans)

    def open(self, path, flag, *mode):
        start_t = time.time()
        MogamiLog.debug("path = %s, flag = %s, mode = %s" %
                        (path, str(flag), str(mode)))
        flag = flag & ~os.O_EXCL
        try:
            fd = os.open(path, flag, *mode)
            ans = 0
        except Exception, e:
            fd = None
            ans = e.errno
        end_t = time.time()
        self.c_channel.open_answer(ans, fd, end_t - start_t)

    def read(self, fd, blnum):
        MogamiLog.debug("fd = %d, bl_num = %d" % (fd, blnum))

        sendbuf = ""
        try:
            os.lseek(fd, blnum * conf.blsize, os.SEEK_SET)
            buf = cStringIO.StringIO()
            readlen = 0
            while readlen < conf.blsize - 1:
                os.lseek(fd, blnum * conf.blsize + readlen, os.SEEK_SET)
                tmpbuf = os.read(fd, conf.blsize - readlen)
                if tmpbuf == '':   # end of file
                    break
                buf.write(tmpbuf)
                readlen += len(tmpbuf)
            sendbuf = buf.getvalue()
            ans = 0
        except Exception, e:
            MogamiLog.error("read have an error (%s)" % (e))
            ans = e.errno

        self.c_channel.data_send(ans, blnum, len(sendbuf), sendbuf)

    def prefetch(self, fd, blnum_list):
        for blnum in blnum_list:
            try:
                sendbuf = ""
                start_t = time.time()
                MogamiLog.debug("fd = %d, blnum = %d" % (fd, bl_num))
                os.lseek(fd, bl_num * conf.blsize, os.SEEK_SET)

                buf = cStringIO.StringIO()
                readlen = 0
                while readlen < conf.blsize - 1:
                    os.lseek(fd, bl_num * conf.blsize + readlen, os.SEEK_SET)
                    tmpbuf = os.read(fd, conf.blsize - readlen)
                    if tmpbuf == '':   # end of file
                        break
                    buf.write(tmpbuf)
                    readlen += len(tmpbuf)
                sendbuf = buf.getvalue()
                end_t = time.time()

                # send data read from file (only in case w/o error)
                self.c_channel.data_send(0, blnum, len(sendbuf), sendbuf)
            except Exception, e:
                MogamiLog.error("Prefetch Error!! with %d-th block" % (blnum))

    def flush(self, fd, listlen, datalen):
        MogamiLog.debug("fd=%d, listlen=%d, datalen=%d" %
                        (fd, listlen, datalen))

        (write_list, buf) = self.c_channel.flush_recv_data(listlen, datalen)
        if len(write_list) != 0:
            write_len = 0
            for wd in write_list:
                try:
                    ans = 0
                    os.lseek(fd, wd[0], os.SEEK_SET)
                    ret = os.write(fd, buf[write:write + wd[1]])
                    write_len += ret
                    if ret != wd[1]:
                        MogamiLog.error("write length error !!")
                        break
                    MogamiLog.debug("write from offset %d (result %d)" %
                                    (wd[0], ret))
                except Exception, e:
                    ans = e.errno
                    break

            self.c_channel.flush_answer(ans, write_len)

    def release(self, fd):
        try:
            os.fsync(fd)
            st = os.fstat(fd)
            os.close(fd)
            size = st.st_size
            ans = 0
        except Exception, e:
            ans = e.errno
            size = 0

        self.c_channel.release_answer(ans, size)

    # MogamiSystem APIs
    def filedel(self, file_list):
        try:
            for file_name in file_list:
                os.unlink(file_name)
            ans = 0
        except Exception, e:
            ans = e.errno

        self.c_channel.filedel_answer(ans)


class MogamiData(object):
    """This is the class of mogami's data server
    """
    def __init__(self, metaaddr, rootpath, mogami_dir):
        """This is the function of MogamiMeta's init.

        @param metaaddr ip address or hostname of metadata server
        @param rootpath path of directory to store data into
        @param mogami_dir path of mogami's root directory
        """
        # basic information of metadata server
        self.metaaddr = metaaddr
        self.rootpath = os.path.abspath(rootpath)
        self.mogami_dir = mogami_dir

        # check directory for data files
        assert os.access(self.rootpath, os.R_OK and os.W_OK and os.X_OK)

        # Initialization of Log.
        MogamiLog.init("data", conf.data_loglevel)
        MogamiLog.info("Start initialization...")
        MogamiLog.debug("rootpath = " + self.rootpath)

        # At first, connect to metadata server and send request to attend.
        self.m_channel = Channel.MogamiChanneltoMeta()
        self.m_channel.connect(self.metaaddr)
        MogamiLog.debug("Success in creating connection to metadata server")
        self.m_channel.dataadd_req(self.rootpath)

        MogamiLog.debug("Init complete!!")

    def run(self, ):
        # create a socket to listen and accept
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lsock.bind(("0.0.0.0", conf.dataport))
        self.lsock.listen(10)
        MogamiLog.debug("Listening on the port " + str(conf.dataport))

        # create a thread to collect dead daemon threads
        daemons = []
        collector_thread = System.MogamiThreadCollector(daemons)
        collector_thread.start()
        threads_count = 0

        while True:
            # connected from client
            (csock, address) = self.lsock.accept()
            MogamiLog.debug("accept connnect from " + str(address[0]))

            client_channel = Channel.MogamiChannelforData()
            client_channel.set_socket(csock)

            datad = MogamiDataHandler(client_channel, self.rootpath)
            datad.name = "D%d" % (threads_count)
            threads_count += 1
            datad.start()
            daemons.append(datad)
            MogamiLog.debug("Created thread name = %s (%d-th threads)" %
                            (datad.getName(), threads_count))

    def finalize(self, ):
        if self.m_channel == None:
            self.m_channel = Channel.MogamiChanneltoMeta()
            self.m_channel.connect(self.metaaddr, conf.metaport)
        self.m_channel.datadel_req()
        self.m_channel.finalize()


def main(meta_addr, dir_path, dddfs_dir):
    data = MogamiData(meta_addr, dir_path, dddfs_dir)
    atexit.register(data.finalize)
    data.run()
