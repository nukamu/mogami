#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

# import mogami's original modules
from libs import Channel
from libs import DBMng
from libs import System
from conf import conf
from libs.System import MogamiLog

# import python standard modules
import os
import os.path
import sys
import stat
import errno
import socket
import cPickle
import time
import threading
import Queue
import string
import re
import random


def meta_file_info(path):
    """read metadata
    """
    f = open(path, 'r')
    buf = f.read()
    f.close()
    l = buf.rsplit(',')
    if len(l) != 3:
        return (None, None, None)
    return (l[0], l[1], l[2])


class MogamiSystemInfo(object):
    """This object should be held by metadata server.
    """
    def __init__(self, meta_rootpath, mogami_dir):
        self.meta_rootpath = os.path.abspath(meta_rootpath)

        # information of data servers
        self.data_list = []
        self.data_rootpath = {}

        self.ramfile_list = []
        self.delfile_q = Queue.Queue()

    def add_data_server(self, ip, rootpath):
        """append a data server to Mogami system

        @param ip ip address of node to append
        @param rootpath root directory path of the data server
        """
        if not ip in self.data_list:
            self.data_list.append(ip)
            self.data_rootpath[ip] = rootpath

    def data_rootpath(self, ip):
        """
        """
        try:
            return self.data_rootpath[ip]
        except KeyError, e:
            return None

    def remove_data_server(self, ip):
        """remove a data server from Mogami system

        @param ip ip address of node to remove
        @return 0 with success, -1 with error
        """
        try:
            self.data_list.remove(ip)
            del self.data_rootpath[ip]
            return True
        except Exception, e:
            MogamiLog.error("cannot find %s from data servers" % (ip))
            return False

    def register_ramfiles(self, add_files_list):
        """

        @param add_files_list
        @return
        """
        self.ramfile_list.extend(add_files_list)


class MogamiMetaHandler(System.MogamiDaemons):
    """This is the class for thread created for each client.
    This handler is run as multithread.
    """
    def __init__(self, sysinfo, client_channel):
        System.MogamiDaemons.__init__(self)
        self.sysinfo = sysinfo
        self.client_channel = client_channel
        self.rootpath = sysinfo.meta_rootpath

    def run(self, ):
        while True:
            req = self.client_channel.recv_request()
            if req == None:
                MogamiLog.debug("Connection closed")
                self.client_channel.finalize()
                break

            if req[0] == Channel.REQ_GETATTR:
                MogamiLog.debug("** getattr **")
                self.getattr(self.rootpath + req[1])

            elif req[0] == Channel.REQ_READDIR:
                MogamiLog.debug("** readdir **")
                self.readdir(self.rootpath + req[1])

            elif req[0] == Channel.REQ_ACCESS:
                MogamiLog.debug("** access **")
                self.access(self.rootpath + req[1], req[2])

            elif req[0] == Channel.REQ_MKDIR:
                MogamiLog.debug("** mkdir **")
                self.mkdir(self.rootpath + req[1], req[2])

            elif req[0] == Channel.REQ_RMDIR:
                MogamiLog.degbug("** rmdir **")
                self.rmdir(self.rootpath + req[1])

            elif req[0] == Channel.REQ_UNLINK:
                MogamiLog.debug("** unlink **")
                self.unlink(self.rootpath + req[1])

            elif req[0] == Channel.REQ_RENAME:
                MogamiLog.debug("** rename **")
                self.rename(self.rootpath + req[1],
                            self.rootpath + req[2])

            elif req[0] == Channel.REQ_MKNOD:
                MogamiLog.debug("** mknod **")
                self.mknod(self.rootpath + req[1], req[2], req[3])

            elif req[0] == Channel.REQ_CHMOD:
                MogamiLog.debug("** chmod **")
                self.chmod(self.rootpath + req[1], req[2])

            elif req[0] == Channel.REQ_CHOWN:
                MogamiLog.debug("** chown **")
                self.chown(self.rootpath + req[1], req[2], req[3])

            elif req[0] == Channel.REQ_LINK:
                self.link(self.rootpath + req[1],
                          self.rootpath + req[2])

            elif req[0] == Channel.REQ_SYMLINK:
                self.symlink(self.rootpath + req[1],
                             self.rootpath + req[2])

            elif req[0] == Channel.REQ_READLINK:
                self.readlink(self.rootpath + req[1])

            elif req[0] == Channel.REQ_TRUNCATE:
                self.truncate(self.rootpath + req[1], req[2])

            elif req[0] == Channel.REQ_UTIME:
                self.utime(self.rootpath + req[1], req[2])

            elif req[0] == Channel.REQ_FSYNC:
                self.fsync(self.rootpath + req[1], req[2])

            elif req[0] == Channel.REQ_OPEN:
                self.open(self.rootpath + req[1], req[2], req[3])

            elif req[0] == Channel.REQ_RELEASE:
                MogamiLog.debug("** release **")
                self.release(req[1], req[2])

            elif req[0] == Channel.REQ_FGETATTR:
                self.fgetattr(req[1])

            elif req[0] == Channel.REQ_FTRUNCATE:
                MogamiLog.error("** ftruncate **")

            elif req[0] == Channel.REQ_DATAADD:
                MogamiLog.debug("** dataadd **")
                ip = self.sock.getpeername()[0]
                self.data_add(ip, req[1])

            elif req[0] == Channel.REQ_DATADEL:
                MogamiLog.debug("** datadel **")
                ip = self.sock.getpeername()[0]
                self.data_del(ip)

            elif req[0] == Channel.REQ_RAMFILEADD:
                MogamiLog.debug("** ramfile add **")
                self.register_ramfiles(req[1])

            elif req[0] == Channel.REQ_FILEASK:
                print '** fileask'
                self.file_ask(req[1])

            else:
                MogamiLog.error("[error] Unexpected Header")
                self.c_channel.finalize()
                break

    # MogamiSystem APIs
    def data_add(self, ip, rootpath):
        self.sysinfo.add_data_server(ip, rootpath)

        print "add data server IP:", ip
        print "Now %d data servers are." % len(self.sysinfo.data_list)
        MogamiLog.info("delete data server IP:" % ip)
        MogamiLog.info("Now there are %d data servers." %
                       len(self.sysinfo.data_list))

    def data_del(self, ip):
        ret = self.sysinfo.remove_data_server(ip)

        if ret == True:
            print "delete data server IP:", ip
            print "Now %d data servers are." % len(self.sysinfo.data_list)
            MogamiLog.info("delete data server IP:" % ip)
            MogamiLog.info("Now there are %d data servers." %
                           len(self.sysinfo.data_list))

    def register_ramfiles(self, add_file_list):
        """register files in list to files to manage on memory

        @param add_file_list
        """
        ramfile_list.extend(add_file_list)

        MogamiLog.debug("** register ramfiles **")
        MogamiLog.debug("add files = " + str(add_file_list))

    def remove_ramfiles(self, file_list):
        """
        """
        pass

    # Mogami's actual metadata access APIs
    def getattr(self, path):
        MogamiLog.debug("path = %s" % path)
        try:
            st = os.lstat(path)
        except os.error, e:
            MogamiLog.debug("stat error!")
            senddata = (e.errno, None)
            self.c_channel.send_header(senddata)
            return

        if os.path.isfile(path):
            try:
                senddata = (0, st, string.atol(l[2]))
            except Exception, e:
                MogamiLog.error()
        else:
            senddata = (0, st, -1)
        self.c_channel.send_header(senddata)

    def readdir(self, path):
        MogamiLog.debug('path=%s' % (path))
        try:
            l = os.listdir(path)
            senddata = [0, l]
        except os.error, e:
            senddata = [e.errno, "null"]
            print 'error!'
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def access(self, path, mode):
        MogamiLog.debug('path=%s' % (path))
        try:
            if os.access(path, mode) == True:
                senddata = True
            else:
                senddata = False
        except os.error, e:
            senddata = False
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def mkdir(self, path, mode):
        MogamiLog.debug("path=%s mode=%o" % (path, mode))
        try:
            os.mkdir(path, mode)
            senddata = 0
        except os.error, e:
            senddata = e.errno
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def rmdir(self, path):
        MogamiLog.debug("path=%s" % (path))
        try:
            os.rmdir(path)
            senddata = 0
        except os.error, e:
            senddata = e.errno
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def unlink(self, path):
        # TODO
        MogamiLog.debug("path = %s" % path)
        if os.path.isfile(path):
            try:
                f = open(path, 'r')
                buf = f.read()
                f.close()
                l = buf.rsplit(',')
                if l[0] in delfile_dict:
                    delfile_dict[l[0]].append(l[1])
                else:
                    delfile_dict[l[0]] = [l[1], ]
            except Exception, e:
                ans = e.errno
        try:
            os.unlink(path)
            senddata = 0
        except os.error, e:
            senddata = e.errno

        self.c_channel.send_header(senddata)

    def rename(self, oldpath, newpath):
        MogamiLog.debug(oldpath + ' -> ' + newpath)
        try:
            os.rename(oldpath, newpath)
            senddata = 0
        except os.error, e:
            senddata = e.errno
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def chmod(self, path, mode):
        MogamiLog.debug('path=%s w/ mode %o' % (path, mode))
        try:
            os.chmod(path, mode)
            senddata = 0
        except os.error, e:
            senddata = e.errno
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def chown(self, path, uid, gid):
        MogamiLog.debug("path=%s uid=%d gid=%d" % (path, uid, gid))
        try:
            os.chown(path, uid, gid)
            senddata = 0
        except os.error, e:
            senddata = e.errno
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def truncate(self, path, len):
        """truncate handler.

        @param path file path
        @param len length of output file
        """
        MogamiLog.debug("path = %s, length = %d" % (path, len))
        try:
            #TODO: exception handling
            f = open(path, 'r+')
            buf = f.read()
            l = buf.rsplit(',')
            buf = "%s,%s,%s" % (l[0], l[1], str(len))
            f.truncate(0)
            f.seek(0)
            f.write(buf)
            f.close()
            senddata = [0, l[0], l[1]]
        except IOError, e:
            senddata = [e.errno, ]
        except Exception, e:
            senddata = [e.errno, ]
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def utime(self, path, times):
        MogamiLog.debug("path = %s, times = %s" % (path, str(times)))
        try:
            os.utime(path, times)
            senddata = 0
        except os.error, e:
            senddata = e.errno
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def open(self, path, flag, mode):
        """open handler.

        @param path file path
        @param flag flags for open(2)
        @param mode open mode (may be empty tuple): actual value is mode[0]
        """
        if os.access(path, os.F_OK) == True:
            # When the required file exist...
            try:
                MogamiLog.debug("!!find the file %s w/ %o" % (path, flag))
                fd = 0
                if mode:
                    fd = os.open(path, os.O_RDWR, mode[0])
                else:
                    fd = os.open(path, os.O_RDWR)
                    MogamiLog.debug("fd = %d" % (fd))
                buf = os.read(fd, conf.bufsize)
                l = buf.rsplit(',')
                dist = l[0]
                filename = l[1]
                size = string.atol(l[2])
                senddata = [0, dist, fd, size, filename]
            except os.error, e:
                MogamiLog.debug("!!find the file but error for %s (%s)" %
                                (path, e))
                senddata = [e.errno, 'null', 'null', 'null', 'null']
                dist = None

            # case of client has file data
            if dist == self.sock.getpeername()[0]:
                for data in datalist:
                    if data[0] == dist:
                        senddata = [0, 'self', fd, filename]
                        break

            channel.send_header(cPickle.dumps(senddata), self.sock)
        else:
            # creat new file
            MogamiLog.debug("can't find the file so create!!")
            try:
                fd = 0
                if mode:
                    fd = os.open(path, os.O_RDWR | os.O_CREAT, mode[0])
                else:
                    fd = os.open(path, os.O_RDWR | os.O_CREAT)
            except os.error, e:
                print "!! have fatal error @1!! (%s)" % (e)
                raise
            try:
                rand = 0
                if len(datalist) > 0:
                    rand = random.randint(0, len(datalist) - 1)
                else:
                    print "!! There are no data server to create file !!"
                dist = datalist[rand][0]
                filename = ''.join(random.choice(string.letters)
                                   for i in xrange(16))
                if os.path.basename(path) in ramfile_list:
                    # if the file is registered at ramfile list
                    filename = os.path.join("/dev/shm/mogami-tmp",
                                            filename)
                else:
                    filename = os.path.join(datalist[rand][1], filename)
                MogamiLog.debug("filename is %s" % (filename,))
                buf = dist + ',' + filename + ',' + '0'
            except Exception, e:
                print "!! have fatal error @2!! (%s)" % (e)
                raise
            MogamiLog.debug("fd = " + str(fd))

            if conf.write_local == True:
                for data in datalist:
                    if data[0] == self.sock.getpeername()[0]:
                        # if client is also data server
                        dist = data[0]
                        buf = dist + ',' + filename + ',' + '0'
                        break

            try:
                os.write(fd, buf)
                os.fsync(fd)
            except os.error, e:
                print "!! have fatal error @3!! (%s)" % (e)
                raise
            size = 0
            senddata = [0, dist, fd, size, filename]

            # case of client has file data
            if dist == self.sock.getpeername()[0]:
                for data in datalist:
                    if data[0] == dist:
                        senddata = [0, 'self', fd, filename]
                        break

            channel.send_header(cPickle.dumps(senddata), self.sock)

    def release(self, fd, fsize):
        """release handler.

        ToDo: exception handling
        ToDo: calculation of writelen
        @param fd file discripter
        @param writelen size of data to be written
        """
        os.lseek(fd, 0, os.SEEK_SET)
#            print "fd = " + str(fd)
        try:
            buf = os.read(fd, conf.bufsize)
        except os.error, e:
            print "OSError in release (%s)" % (e)
        l = buf.rsplit(',')

        size = string.atol(l[2])
        if size != fsize:
            try:
                buf = l[0] + ',' + l[1] + ',' + str(fsize)
                MogamiLog.debug("write to meta file %s" % buf)

                os.ftruncate(fd, len(buf))
                os.lseek(fd, 0, os.SEEK_SET)
                os.write(fd, buf)
                os.fsync(fd)
            except os.error, e:
                print "OSError in release (%s)" % (e)

        os.close(fd)
        senddata = 0
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def fgetattr(self, fd):
        print fd
        try:
            st = os.fstat(fd)
            senddata = [0, st]
        except os.error, e:
            print "OSError in fgetattr (%s)" % (e)
            senddata = [e.errno, 'null']
        channel.send_header(cPickle.dumps(senddata), self.sock)

    def file_ask(self, path_list):
        dist_dict = {}

        for path in path_list:
            print os.path.join(self.rootpath, path)
            try:
                f = open(os.path.join(self.rootpath, path), 'r')
                buf = f.read()
                l = buf.rsplit(',')
                dist_dict[path] = l[0]
            except Exception:
                pass
        print dist_dict
        senddata = dist_dict
        channel.send_header(cPickle.dumps(senddata), self.sock)


class MogamiDaemononMeta(System.MogamiDaemons):
    """Send the data servers the request to delete files.

    @param files file list to delete
    """
    def __init__(self, sysinfo):
        System.MogamiDaemons.__init__(self)
        self.delfile_q = sysinfo.delfile_q

    def run(self, ):
        while True:
            #del_key_list = []
            #for IP, files in delfile_dict.iteritems():
                #self.send_delete_request(IP, files)
                #del_key_list.append(IP)
            #or del_key in del_key_list:
                #del delfile_dict[del_key]
            time.sleep(3)

    def send_delete_request(self, ip, files):
        senddata = (Channel.REQ_FILEDEL, files)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((IP, conf.dataport))

        channel.send_header(cPickle.dumps(senddata), sock)
        ans = channel.recv_header(sock)
        senddata = ['close']
        channel.send_header(cPickle.dumps(senddata), sock)
        sock.close()


class MogamiMeta(object):
    """This is the class of mogami's metadata server
    """
    def __init__(self, rootpath, mogami_dir):
        """This is the function of MogamiMeta's init.
        In this function,
        """
        MogamiLog.init("meta", MogamiLog.INFO)

        self.sysinfo = MogamiSystemInfo(rootpath, mogami_dir)

        """Check directory for data files.
        """
        if os.access(rootpath, os.R_OK and os.W_OK and os.X_OK) == False:
            sys.exit("%s is not permitted to use. " % (rootpath, ))

        MogamiLog.info("** Mogami metadata server init **")
        MogamiLog.debug("rootpath = " + rootpath)

    def run(self, ):
        """Connected from Mogami Client.
        """
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.bind(("0.0.0.0", conf.metaport))
        self.lsock.listen(10)
        MogamiLog.debug("Listening at the port " + str(conf.metaport))
        daemons = []
        thread_collector = System.MogamiThreadCollector(daemons)
        thread_collector.start()
        threads_count = 0

        delete_files_thread = MogamiDaemononMeta(self.sysinfo)
        delete_files_thread.start()

        while True:
            (client_sock, address) = self.lsock.accept()
            MogamiLog.debug("accept connnect from %s" % (str(address[0])))
            client_channel = Channel.MogamiChannelforMeta()
            client_channel.set_socket(client_sock)
            metad = MogamiMetaHandler(client_channel, self.sysinfo)
            metad.start()
            daemons.append(metad)

            MogamiLog.debug("Created thread name = " + metad.getName())


def main(dir_path, mogami_dir):
    meta = MogamiMeta(dir_path, mogami_dir)
    meta.run()
