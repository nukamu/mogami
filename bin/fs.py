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
from libs.System import MogamiLog

m_channel = Channel.MogamiChanneltoMeta()
daemons = []
file_size_dict = {}


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
            st = tips.MogamiStat()
            st.load(ret_st)
            if fsize >= 0:
                st.chsize(fsize)
            if path in file_size_dict:
                MogamiLog.debug("path = %s, change size from %d to %d" %
                                (path, fsize, file_size_dict[path]))
                st.chsize(file_size_dict[path])
            return st

    def readdir(self, path, offset):
        MogamiLog.debug("** readdir ** path = %s, offset = %s" %
                        (path + str(offset)))

        ans = m_channel.readdir_req(path, offset)
        l = ['.', '..']
        if ans[0] == 0:
            l.extend(ans[1])
            return [fuse.Direntry(ent) for ent in l]
        else:
            return -ans[0]

    def access(self, path, mode):
        MogamiLog.debug("** access **" + path + str(mode))
        ans = m_channel.access_req(path, mode)
        if ans != True:
            return -errno.EACCES

    def mkdir(self, path, mode):
        """mkdir handler.
        
        @param path directory path to mkdir
        @param mode permission of the directory to create
        @return 0 on success, errno on error
        """
        MogamiLog.debug("** mkdir **" + path + str(mode))
        ans = m_channel.mkdir_req(path, mode)
        if ans != 0:
            return -ans

    def rmdir(self, path):
        """rmdir handler.
        """
        MogamiLog.debug("** rmdir **" + path)
        ans = m_channel.rmdir_req(path)
        if ans != 0:
            return -ans

    def unlink(self, path):
        """unlink handler.

        @param path path name to unlink
        """
        MogamiLog.debug("** unlink ** path = %s" % (path, ))
        ans = m_channel.nulink_req(path)
        if ans != 0:
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
        MogamiLog.debug('** truncate ** path = %, length = %d' %
                        (path, length))

        (ans, dest, filename) = m_channel.truncate_req(path, length)
        if ans != 0:
            return -ans
        MogamiLog.debug("send request to data server: path=%s, len=%d" %
                        (path, length))

        c_channel = Channel.MogamiChanneltoData(dest)
        ans = c_channel.truncate_req(path, length)
        c_channel.finalize()

        # if truncate was succeeded, file size should be changed
        if ans == 0:
            file_size_dict[path] = length
        return -ans

    def utime(self, path, times):
        MogamiLog.debug('** utime **' + path + str(times))

        ans = m_channel.utime_req(path, times)
        if ans != 0:
            return -ans

    class MogamiFile(Fuse):
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
            ans = m_channel.open_req(path, flag, *mode)
            MogamiLog.debug("open ans (from meta)" + str(ans))

            # parse answer from metadata server
            dest = ans[1]
            metafd = ans[2]
            size = ans[3]
            data_path = ans[4]

            if ans[1] == 'self':
                self.mogami_file = tips.MogamiLocalFile(
                    path, size, data_path, flag, *mode)
            else:
                self.mogami_file = tips.MogamiRemoteFile(
                    path, size, dest, data_path, flag, *mode)
                (ans, rtt) = self.create_connections()
                if ans != 0:
                    print "open error !!"
                    return
                self.mogami_file.rtt = rtt

            # register file size to file size dictionary
            file_size_dict[path] = size

            """Get Id list to know pid.
            list = {gid: pid: uid}
            And then get the command from pid.
            """
            try:
                id_list = self.GetContext()
                pid = id_list['pid']
                f = open(os.path.join("/proc", str(pid), "cmdline"), 'r')
                self.cmd_args = f.read().rsplit('\x00')[:-1]
                print self.cmd_args
            except Exception, e:
                pass

            self.preth = System.MogamiPrefetchThread(self)
            self.preth.start()
            daemons.append(self.preth)
            self.access_pattern = Tips.MogamiFileAccessPattern()

        def read(self, length, offset):
            """read handler.

            return strings read from file
            @param length request size of read
            @param offset offset of read request
            """
            MogamiLog.debug("**read offset=%d, length=%d" % (offset, length))

            # check access pattern
            self.access_pattern.check_mode(offset, length)

            self.mogami_file.read(length, offset)


            # ask for prefetch data
            start_t = time.time()
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
            end_t = time.time()
            self.calc_time += end_t - start_t

            if conf.prefetch == True:
                self.access_pattern.change_info(offset, ret_str.tell(),
                                                last_readbl)
            return ret_str.getvalue()


        def request_prefetch(self, blnum_list):
            """function to send a request of prefetch.

            @param blnum_list block number list to prefetch
            """
            MogamiLog.debug('** prefetch' + str(blnum_list))
            senddata = ['prefetch', self.datafd, blnum_list]

            with self.r_buflock:
                for blnum in blnum_list:
                    self.bldata[blnum].state = 1

            with self.plock:
                channel.send_header(cPickle.dumps(senddata), self.psock)

        def write(self, buf, offset):
            if self.size < offset + len(buf):
                self.size = offset + len(buf)
                file_size_dict[self.path] = self.size

            if self.remote == True:
                prev_blnum = self.blnum
                # recalculation of the number of blocks
                self.blnum = self.size / conf.blsize
                if self.size % conf.blsize != 0:
                    self.blnum += 1
                if prev_blnum < self.blnum:
                    self.bldata += tuple([Tips.MogamiBlock() for i in
                                        range(self.blnum - prev_blnum)])

            tmp = (offset, len(buf))
            prev_writelen = self.writelen
            with self.w_buflock:
                self.writelist.append(tmp)
                self.writedata.write(buf)
                self.writelen += len(buf)

            reqs = self.cal_bl(offset, len(buf))
            dirty_write = 0
            for req in reqs:
                if req[0] in self.dirty_dict:
                    self.dirty_dict[req[0]].append((req[1], req[2],
                                                    prev_writelen))
                else:
                    self.dirty_dict[req[0]] = [(req[1], req[2],
                                                prev_writelen), ]

            if self.writelen > conf.writelen_max:
                self._fflush()

            return len(buf)

        def _fflush(self, ):
            with self.w_buflock:
                buf = cPickle.dumps(self.writelist)
                data = self.writedata.getvalue()

                senddata = ['flush', self.datafd, len(buf), len(data)]
                MogamiLog.debug("flush: fd=%d, listlen=%d, datalen=%d" %
                                (self.datafd, len(buf), len(data)))
                with self.dlock:
                    channel.send_header(cPickle.dumps(senddata), self.dsock)
                    self.dsock.sendall(buf)
                    self.dsock.sendall(data)
                    ans = channel.recv_header(self.dsock)
                self.writelen = 0
                self.writelist = []
                self.writedata = cStringIO.StringIO()
                self.dirty_dict = {}
            return ans

        def flush(self, ):
            self.mogami_file.flush()
            return 0

        def fsync(self, isfsyncfile):
            self.mogami_file.fsync()
            
        def release(self, flags):
            size = self.mogami_file.release()

            ans = m_channel.relase_req(self.metafd, size)
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