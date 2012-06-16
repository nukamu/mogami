#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)

# python standard modules
import stat
import errno
import threading
import os
import os.path
import sys
import socket
import cPickle
import cStringIO
import select
import string
import re
import time
sys.path.append(os.pardir)

# mogami's original modules
from conf import conf
from libs import Channel
from libs import DBMng
from libs import System
from libs import Tips
from libs import FileManager
from libs.System import MogamiLog


m_channel = Channel.MogamiChanneltoMeta()
daemons = []
file_size_dict = {}
channels = Channel.MogamiChannelRepository()


class MogamitoTellAccessPattern(System.MogamiDaemons):
    def __init__(self, pipepath, queue):
        System.MogamiDaemons.__init__(self)
        self.pipepath = pipepath
        self.queue = queue

    def run(self, ):
        while True:
            time.sleep(100)

class MogamiFS(Fuse):
    """Class for Mogami file system (client)
    """
    def __init__(self, meta_server, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        self.meta_server = meta_server
        self.parse(errex=1)
        m_channel.connect(self.meta_server)

    def fsinit(self, ):
        """Called before fs.main() called.
        """
        # initializer log
        MogamiLog.info("** Mogami FS init **")
        MogamiLog.debug("Success in creating connection to metadata server")
        MogamiLog.debug("Init complete!!")
        
        # create a thread for collecting dead threads
        collector_thread = System.MogamiThreadCollector(daemons)
        collector_thread.start()

        # create a thread for telling access pattern logs
        tellap_thread = MogamitoTellAccessPattern('/tmp/mogami_ap')
        daemons.append(tellap_thread)
        tellap_thread.start()

    def finalize(self, ):
        """Finalizer of Mogami.
        This seems not to be called implicitly...
        """
        m_channel.finalize()
        MogamiLog.info("** Mogami Unmount **")

    # From here functions registered for FUSE are written.
    def mythread(self):
        MogamiLog.debug("** mythread **")
        return -errno.ENOSYS

    def getattr(self, path):
        MogamiLog.debug("** getattr ** path = %s" % (path, ))

        (ans, ret_st, fsize) = m_channel.getattr_req(path)
        if ans != 0:
            return -ans
        else:
            st = FileManager.MogamiStat()
            st.load(ret_st)
            if fsize >= 0:
                st.chsize(fsize)
            # if file_size_dict has cache of file size, replace it
            if path in file_size_dict:
                MogamiLog.debug("path = %s, change size from %d to %d" %
                                (path, fsize, file_size_dict[path]))
                st.chsize(file_size_dict[path])
            return st

    def readdir(self, path, offset):
        MogamiLog.debug("** readdir ** path = %s, offset = %s" %
                        (path, str(offset)))

        (ans, contents) = m_channel.readdir_req(path, offset)
        l = ['.', '..']
        if ans == 0:
            l.extend(contents)
            return [fuse.Direntry(ent) for ent in l]
        else:
            return -ans

    def access(self, path, mode):
        """access handler.

        @param path path to access
        @param mode mode to access
        @return 0 on success, errno on error
        """
        MogamiLog.debug("** access **" + path + str(mode))
        ans = m_channel.access_req(path, mode)
        return -ans

    def mkdir(self, path, mode):
        """mkdir handler.
        
        @param path directory path to mkdir
        @param mode permission of the directory to create
        @return 0 on success, errno on error
        """
        MogamiLog.debug("** mkdir **" + path + str(mode))
        ans = m_channel.mkdir_req(path, mode)
        return -ans

    def rmdir(self, path):
        """rmdir handler.
        """
        MogamiLog.debug("** rmdir **" + path)
        ans = m_channel.rmdir_req(path)
        return -ans

    def unlink(self, path):
        """unlink handler.

        @param path path name to unlink
        """
        MogamiLog.debug("** unlink ** path = %s" % (path, ))
        ans = m_channel.unlink_req(path)
        return -ans

    def rename(self, oldpath, newpath):
        """rename handler.

        @param oldpath original path name before rename
        @param newpath new path name after rename
        """
        MogamiLog.debug("** rename ** oldpath = %s, newpath = %s" %
                        (oldpath, newpath))
        ans = m_channel.rename_req(oldpath, newpath)
        if ans != 0:
            return -ans

    def chmod(self, path, mode):
        """chmod handler.

        @param path path to change permission of
        @param mode permission to change
        """
        MogamiLog.debug("** chmod ** path = %s, mode = %s" %
                        (path, oct(mode)))
        ans = m_channel.chmod_req(path, mode)
        if ans != 0:
            return -ans

    def chown(self, path, uid, gid):
        MogamiLog.debug('** chown ** ' + path + str(uid) + str(gid))
        ans = m_channel.chown_req(path, uid, gid)
        if ans != 0:
            return -ans

    def truncate(self, path, length):
        MogamiLog.debug('** truncate ** path = %s, length = %d' %
                        (path, length))

        (ans, dest, filename) = m_channel.truncate_req(path, length)
        if ans != 0:
            return -ans

        c_channel = Channel.MogamiChanneltoData(dest)
        ans = c_channel.truncate_req(filename, length)
        c_channel.finalize()

        # if truncate was succeeded, cache of file size should be changed
        if ans == 0:
            file_size_dict[path] = length
        return -ans

    def utime(self, path, times):
        MogamiLog.debug('** utime **' + path + str(times))
        ans = m_channel.utime_req(path, times)
        if ans != 0:
            return -ans

    class MogamiFile(object):
        """This is the class of file management on Mogami.
        """

        def __init__(self, path, flag, *mode):
            """Initializer called when opened.

            @param path file path
            @param flag flags with open(2)
            @param *mode file open mode (may not be specified)
            """
            MogamiLog.debug("** open ** path = %s, flag = %s, mode = %s" %
                            (path, str(flag), str(mode)))
            # parse argurments
            self.path = path
            self.flag = flag
            self.mode = mode

            (ans, dest, self.metafd, data_path, self.fsize,
             self.created) = m_channel.open_req(path, flag, *mode)

            if ans != 0:  # error on metadata server
                e = IOError()
                e.errno = ans
                raise e

            if dest == 'self':
                self.mogami_file = FileManager.MogamiLocalFile(
                    self.fsize, data_path, flag, *mode)
            else:
                self.mogami_file = FileManager.MogamiRemoteFile(
                    self.fsize, dest, data_path, flag, *mode)
                ans = self.mogami_file.create_connections(channels)
                if ans != 0:
                    MogamiLog.error("open error !!")
                    e = IOError()
                    e.errno = ans
                    raise e

            # register file size to file size dictionary
            file_size_dict[path] = self.fsize

            """Get Id list to know pid.
            list = {gid: pid: uid}
            And then get the command from pid.
            """
            """try:
                id_list = self.GetContext()
                pid = id_list['pid']
                f = open(os.path.join("/proc", str(pid), "cmdline"), 'r')
                self.cmd_args = f.read().rsplit('\x00')[:-1]
                print self.cmd_args
            except Exception, e:
                # with any error, pass this part of process
                pass"""

            if self.mogami_file.remote == True:
                self.recvth = System.MogamiPrefetchThread(self.mogami_file)
                self.recvth.start()
                daemons.append(self.recvth)

            #self.access_pattern = Tips.MogamiFileAccessPattern()

        def read(self, length, offset):
            """read handler.

            return strings read from file
            @param length request size of read
            @param offset offset of read request
            """
            MogamiLog.debug("**read offset=%d, length=%d" % (offset, length))

            # check access pattern
            #self.access_pattern.check_mode(offset, length)
            #self.access_pattern.change_info(offset, ret_str.tell(),
#                                            last_readbl)

            """
            if conf.prefetch == True:
                if last_readbl != self.access_pattern.last_bl:
                    #MogamiLog.debug("read: prenum %d \n" % (self.prenum))
                    if conf.force_prenum == True:
                        self.prenum = conf.prenum
                    blnum_list = []
                    prereq_list = self.access_pattern.return_need_block(
                        (last_readbl + 1) * 1024 * 1024, self.prenum,
                        self.blnum)
                    for prereq in prereq_list:
                        if prereq > self.blnum:
                            break
                        if self.bldata[prereq].state == 0:
                            blnum_list.append(prereq)
                    if len(blnum_list) != 0:
                        self.request_prefetch(blnum_list)
            self.calc_time += end_t - start_t"""

            return self.mogami_file.read(length, offset)

        def write(self, buf, offset):
            if self.fsize < offset + len(buf):
                self.fsize = offset + len(buf)
                file_size_dict[self.path] = self.fsize
            return self.mogami_file.write(buf, offset)

        def flush(self, ):
            ans = self.mogami_file.flush()
            return ans

        def fsync(self, isfsyncfile):
            ans = self.mogami_file.fsync(isfsyncfile)
            return ans

        def release(self, flags):
            MogamiLog.debug("** release **")

            fsize = self.mogami_file.release(flags)

            ans = m_channel.release_req(self.metafd, fsize)
            # delete file size cache
            if self.path in file_size_dict:
                del file_size_dict[self.path]
            return 0

    def main(self, *a, **kw):
        """This is the main method of MogamiFS.
        """
        self.file_class = self.MogamiFile
        return Fuse.main(self, *a, **kw)


if __name__ == "__main__":
    MogamiLog.init("fs", conf.fs_loglevel)
    fs = MogamiFS(sys.argv[1],
                  version="%prog " + fuse.__version__,
                  usage=System.usagestr(), )
    fs.flags = 0
    fs.multithreaded = conf.multithreaded
    fs.main()
    fs.finalize()
