#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

import os
import sys
sys.path.append(os.pardir)
import threading
import cStringIO
import time
import os.path
import Queue

from conf import conf
from libs.System import MogamiLog
from fuse import Fuse
from libs import Channel
import fuse
fuse.fuse_python_api = (0, 2)


class MogamiAccessPattern(object):
    """Access pattern repository for a file
    """
    read = 0
    write = 1

    def __init__(self, path, cmds, pid):
        self.path = path
        self.cmd_args = cmds
        self.pid = pid
        self.queue = Queue.Queue()

    def insert_data(self, ops, offset, length):
        self.queue.put((ops, offset, length))
            
    def mk_form_data(self, ):
        read_data = []
        write_data = []
        while True:
            try:
                item = self.queue.get_nowait()
                if item[0] == self.read:
                    read_data.append((item[1], item[2]))
                else:
                    write_data.append((item[1], item[2]))
            except Queue.Empty:
                break
        
        read_data.sort()
        write_data.sort()

        tmp_list = []
        next_expected_offset = -1
        for data in read_data:
            if data[0] == next_expected_offset:
                former_data = tmp_list.pop()
                tmp_list.append((former_data[0], former_data[1] + data[1]))
            else:
                tmp_list.append(data)
            next_expected_offset = data[0] + data[1]
        read_data = tmp_list

        tmp_list = []
        next_expected_offset = -1
        for data in write_data:
            if data[0] == next_expected_offset:
                former_data = tmp_list.pop()
                tmp_list.append((former_data[0], former_data[1] + data[1]))
            else:
                tmp_list.append(data)
            next_expected_offset = data[0] + data[1]
        write_data = tmp_list

        return (read_data, write_data)


class MogamiStat(fuse.Stat):
    attrs = ("st_mode", "st_ino", "st_dev", "st_nlink",
             "st_uid", "st_gid", "st_size",
             "st_atime", "st_mtime", "st_ctime")
    mogami_attrs = ("st_mode", "st_uid", "st_gid", "st_nlink",
                    "st_size", "st_atime", "st_mtime", "st_ctime")

    def __init__(self, ):
        for attr in self.mogami_attrs:
            try:
                #setattr(self, "_" + attr, 0)
                setattr(self, attr, 0)
            except AttributeError, e:
                print e

    def __repr__(self):
        s = ", ".join("%s=%d" % (attr, getattr(self, "_" + attr))
                      for attr in self.mogami_attrs)
        return "<MogamiStat %s>" % (s,)

    def load(self, st):
        for attr in self.attrs:
            try:
                #setattr(self, attr, getattr(st, "_" + attr))
                setattr(self, attr, getattr(st, attr))
            except AttributeError, e:
                print e

    #def __getattr__(self, attrname):
    #    val = getattr(self, "_" + attrname)
    #    return val

    #def __setattr__(self, attrname, newvalue):
    #    setattr(self, "_" + attrname, newvalue)

    # TODO: Deprecated
    def chsize(self, size):
        self.st_size = size


class MogamiBlock(object):
    """Class of a object of a block
    """

    def __init__(self, ):
        self.state = 0
        """Data are..
        0: not exist
        1: requesting
        2: exist
        """
        # block data
        self.buf = ""

class MogamiFile(object):
    """Class for a object of a file
    """
    def __init__(self, fsize):
        self.fsize = fsize

class MogamiRamFile(object):
    """Class for managing a content of file on memory.

    This class is not implemented completely.
    """

    def __init__(self, size):
        self.buf = cStringIO.StringIO()
        self.block_num = 0
        self.errno = 0

    def read(self, length):
        self.buf.read(length)

    def write(self, offset, buf):
        
        pass


class MogamiRemoteFile(MogamiFile):
    """Class for a file located at remote node
    """
    def __init__(self, fsize, dest, data_path, flag, *mode):
        MogamiFile.__init__(self, fsize)
        self.remote = True
        self.dest = dest
        self.data_path = data_path
        self.flag = flag
        self.mode = mode

        self.prenum = 1

        # calculation of the number of blocks
        self.blnum = self.fsize / conf.blsize
        if self.fsize % conf.blsize != 0:
            self.blnum += 1

        # initialization of read buffer
        self.r_data = tuple([MogamiBlock() for i in range(self.blnum + 1)])
        self.r_buflock = threading.Lock()
        MogamiLog.debug("create r_data 0-%d block" % (len(self.r_data)-1))

        # initialization of write buffer
        self.w_list = []
        self.w_data = cStringIO.StringIO()
        self.w_buflock = threading.Lock()
        self.w_len = 0
        # for buffer of dirty data
        self.dirty_dict = {}
        
    def create_connections(self, channel_repository):
        """Create connections to data server which has file contents.

        In this function, send request for open to data server.
        (and calculate RTT)
        """
        channels = channel_repository.get_channel(self.dest)
        self.d_channel = Channel.MogamiChanneltoData(self.dest)
        # create a connection for prefetching
        #self.p_channel = Channel.MogamiChanneltoData(self.dest)
        #channel_repository.set_channel(self.dest, self.d_channel, self.p_channel)
        #else:
            # set channels
        #    self.d_channel = channels[0]
        #    self.p_channel = channels[1]

        # send a request to data server for open
        start_t = time.time()
        (ans, self.datafd, open_t) = self.d_channel.open_req(
            self.data_path, self.flag, *self.mode)
        end_t = time.time()
        if ans != 0:  # failed...with errno
            self.finalize()
            return ans

        # on success
        self.rtt = end_t - start_t - open_t
        # must be 0
        return ans

    def finalize(self, ):
        self.r_data = None
        self.d_channel.finalize()
        #self.p_channel.close_req()
        #self.p_channel.finalize()

    def read(self, length, offset):
        requestl = self.cal_bl(offset, length)

        # prepare buffer to return
        ret_str = cStringIO.StringIO()
        MogamiLog.debug("requestl = %s, with offset: %d, length: %d" %
                        (str(requestl), offset, length))
        for req in requestl:
            reqbl = req[0]
            buf = ""     # buffer for block[reqbl]
            last_readbl = reqbl

            if self.r_data[reqbl].state == 2:
                buf = self.r_data[reqbl].buf
            elif self.r_data[reqbl].state == 1:
                MogamiLog.debug("Waiting recv data %d block" % reqbl)
                while self.r_data[reqbl].state == 1:
                    time.sleep(0.01)
                buf = self.r_data[reqbl].buf
            else:
                self.request_block(reqbl)
                #while self.r_data[reqbl].state == 1:
                #    time.sleep(0.01)
                with self.r_buflock:
                    buf = self.r_data[reqbl].buf

            # check dirty data (and may replace data)
            with self.w_buflock:
                if reqbl in self.dirty_dict:
                    dirty_data_list = self.dirty_dict[reqbl]
                    for dirty_data in dirty_data_list:
                        # remember the seek pointer of write buffer
                        seek_point = self.w_data.tell()
                        self.w_data.seek(dirty_data[2])
                        dirty_str = self.w_data.read(
                            dirty_data[1] - dirty_data[0])
                        self.w_data.seek(seek_point)
                        tmp_buf = buf[dirty_data[1]:]
                        buf = buf[0:dirty_data[0]] + dirty_str + tmp_buf

            # write strings to return and if reach EOF, break
            ret_str.write(buf[req[1]:req[2]])
            if len(buf) < conf.blsize:
                break    # end of file

        return ret_str.getvalue()

    def write(self, buf, offset):
        # recalculation of the number of blocks
        prev_blnum = self.blnum
        self.blnum = self.fsize / conf.blsize
        if self.fsize % conf.blsize != 0:
            self.blnum += 1
        if prev_blnum < self.blnum:
            self.r_data += tuple([MogamiBlock() for i in
                                  range(self.blnum - prev_blnum)])

        tmp = (offset, len(buf))
        prev_writelen = self.w_len
        with self.w_buflock:
            self.w_list.append(tmp)
            self.w_data.write(buf)
            self.w_len += len(buf)

        reqs = self.cal_bl(offset, len(buf))
        dirty_write = 0
        for req in reqs:
            if req[0] in self.dirty_dict:
                self.dirty_dict[req[0]].append((req[1], req[2],
                                                prev_writelen))
            else:
                self.dirty_dict[req[0]] = [(req[1], req[2],
                                            prev_writelen), ]
        if self.w_len > conf.writelen_max:
            self.flush()
        return len(buf)

    def flush(self, ):
        if self.w_len == 0:
            return 0
        with self.w_buflock:
            send_data = self.w_data.getvalue()

            MogamiLog.debug("flush: fd=%d, listlen=%d, datalen=%d" %
                            (self.datafd, len(self.w_list), len(send_data)))
            ans = self.d_channel.flush_req(self.datafd, self.w_list, send_data)
            self.w_len = 0
            self.w_list = []
            self.w_data = cStringIO.StringIO()
            self.dirty_dict = {}
        return ans

    def fsync(self, isfsyncfile):
        if self.w_len == 0:
            pass
        else:
            self.flush()

    def release(self, flags):
        if self.w_len != 0:
            self.flush()
        (ans, fsize) = self.d_channel.release_req(self.datafd)
        self.finalize()
        return fsize

    def request_block(self, blnum):
        """send request of block data.
        
        @param blnum the number of block to require
        """
        MogamiLog.debug("** read %d block" % (blnum))
        MogamiLog.debug("request to data server %d block" % (blnum))

        # change status of the block (to requiring)
        with self.r_buflock:
            self.r_data[blnum].state = 1
       
        bldata = self.d_channel.read_req(self.datafd, blnum)

        if bldata == None:
            self.r_data = None       

        with self.r_buflock:
            self.r_data[blnum].state = 2
            self.r_data[blnum].buf = bldata

    def request_prefetch(self, blnum_list):
        """send request of prefetch.

        @param blnum_list the list of block numbers to prefetch
        """
        MogamiLog.debug("** prefetch ** required blocks = %s" % 
                        (str(blnum_list)))
        # send request to data server
        self.p_channel.prefetch_req(self.datafd, blnum_list)

        with self.r_buflock:
            for blnum in blnum_list:
                self.r_data[blnum].state = 1

    def cal_bl(self, offset, size):
        """This function is used for calculation of request block number.

        return list which show required block number.
        @param offset offset of read request
        """
        blbegin = offset / conf.blsize
        blend = (offset + size - 1) / conf.blsize + 1
        blnum = range(blbegin, blend)

        blfrom = [offset % conf.blsize, ]
        blfrom.extend([0 for i in range(len(blnum) - 1)])

        blto = [conf.blsize for i in range(len(blnum) - 1)]
        least = (offset + size) % conf.blsize

        if least == 0:
            least = conf.blsize
        blto.append(least)

        return zip(blnum, blfrom, blto)


class MogamiLocalFile(MogamiFile):
    """Class for a file located at local storage
    """
    def __init__(self, fsize, data_path, flag, *mode):
        MogamiFile.__init__(self, fsize)
        self.remote = False
        
        # make information for open
        md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
        m = md[flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]
        if flag | os.O_APPEND:
            m = m.replace('w', 'a', 1)

        # open the file actually
        self.file = os.fdopen(os.open(data_path, flag, *mode), m)
        self.lock = threading.Lock()

    def read(self, length, offset):
        with self.lock:
            # read data from local file
            self.file.seek(offset)
            return self.file.read(length)

    def write(self, buf, offset):
        with self.lock:
            # write data to local file
            self.file.seek(offset)
            self.file.write(buf)
        return len(buf)

    def flush(self, ):
        if 'w' in self.file.mode or 'a' in self.file.mode:
            self.file.flush()
        return 0

    def fsync(self, isfsyncfile):
        if 'w' in self.file.mode or 'a' in self.file.mode:
            self.file.flush()
        if isfsyncfile and hasattr(os, 'fdatasync'):
            os.fdatasync(self.file.fileno())
        else:
            os.fsync(self.file.fileno())
        return 0

    def release(self, flags):
        st = os.fstat(self.file.fileno())
        filesize = st.st_size
        self.file.close()
        return st.st_size

    def finalize(self, ):
        self.file.close()
