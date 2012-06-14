#! /usr/bin/env python
#-*- coding: utf-8 -*-

import os

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)


class MogamiStat(fuse.Stat):
    attrs = ("st_mode", "st_ino", "st_dev", "st_nlink",
             "st_uid", "st_gid", "st_size",
             "st_atime", "st_mtime", "st_ctime")
    mogami_attrs = ("st_mode", "st_uid", "st_gid", "st_nlink",
                    "st_size", "st_atime", "st_mtime", "st_ctime")

    def __init__(self, ):
        for attr in self.mogami_attrs:
            try:
                setattr(self, attr, 0)
            except AttributeError, e:
                print e

    def __repr__(self):
        s = ", ".join("%s=%d" % (attr, getattr(self, attr))
                      for attr in self.attrs)
        return "<MogamiStat %s>" % (s,)

    def load(self, st):
        for attr in self.attrs:
            try:
                setattr(self, attr, getattr(st, attr))
            except AttributeError, e:
                print e
                pass

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
    def __init__(self, size):
        self.size = size

class MogamiRamFile(object):
    """Class for managing a content of file on memory.
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
    def __init__(self, path, fsize, dest, data_path, created, flag, *mode):
        MogamiFile.__init__(self, path, fsize)
        self.dest = dest
        self.data_path = data_path
        self.flag = flag
        self.mode = mode
        self.remote = True
        self.prenum = 1
        self.created = created

        # initialization of read buffer
        self.blnum = self.fsize / conf.blsize
        if self.fsize % conf.blsize != 0:
            self.blnum += 1
        self.bldata = tuple([MogamiBlock() for i in range(self.blnum + 1)])
        self.r_buflock = threading.Lock()
        MogamiLog.debug("create bldata 0-%d block" % (len(self.bldata)-1))

        # initialization of write buffer
        self.w_buflock = threading.Lock()
        self.w_list = []
        self.w_data = cStringIO.StringIO()
        self.w_len = 0
        # for buffer of dirty data
        self.dirty_dict = {}
        
    def create_connections(self, ):
        """
        """
        # create a connection for data transfer
        try:
            self.d_channel = Channel.MogamiChanneltoData(self.dest)
        except Exception, e:
            return e.errno
        # create a connection for prefetching
        try:
            self.p_channel = Channel.MogamiChanneltoData(self.dest)
        except Exception, e:
            return e.errno

        # send a request to data server for open
        start_t = time.time()
        (ans, self.datafd, open_t) = self.d_channel.open_req(
            self.data_path, self.flag, self.mode)
        end_t = time.time()
        if ans != 0:  # failed...with errno
            self.finalize()
            return ans

        # on success
        self.rtt = end_t - start_t - open_t
        return ans

    def finalize(self, ):
        pass

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

            if self.bldata[reqbl].state == 2:
                buf = self.bldata[reqbl].buf
            elif self.bldata[reqbl].state == 1:
                MogamiLog.debug("Waiting recv data %d block" % reqbl)
                while self.bldata[reqbl].state == 1:
                    time.sleep(0.01)
                buf = self.bldata[reqbl].buf
            else:
                ret = self.request_block(reqbl)
                if ret != 0:
                    MogamiLog.error("read error! (%d)" % ret)
                    return ret
                while self.bldata[reqbl].state == 1:
                    time.sleep(0.01)
                with self.r_buflock:
                    buf = self.bldata[reqbl].buf

            # check dirty data (and may replace data)
            with self.w_buflock:
                if reqbl in self.dirty_dict:
                    dirty_data_list = self.dirty_dict[reqbl]
                    for dirty_data in dirty_data_list:
                        # remember the seek pointer
                        seek_point = self.writedata.tell()
                        self.writedata.seek(dirty_data[2])
                        dirty_str = self.writedata.read(
                            dirty_data[1] - dirty_data[0])
                        self.writedata.seek(seek_point)
                        tmp_buf = buf[dirty_data[1]:]
                        buf = buf[0:dirty_data[0]] + dirty_str + tmp_buf

            # write strings to return and if reach EOF, break
            ret_str.write(buf[req[1]:req[2]])
            if len(buf) < conf.blsize:
                break    # end of file

    def write(self, buf, offset):
        
        pass

    def flush(self, ):
        pass

    def fsync(self, ):
        if self.w_len == 0:
            pass
        else:
            self.flush()

    def release(self, ):
        if self.w_len == 0:
            pass
        else:
            self.flush()
        channel.send_header(cPickle.dumps(senddata), self.dsock)
        ans = channel.recv_header(self.dsock)
        MogamiLog.debug('** release')
        
        self.bldata = None
        self.timedic = None
        self.channel.finalize()
        self.p_chanel.finalize()

    def request_block(self, blnum):
        """send request of block data.
        
        @param blnum the number of block to require
        """

        senddata = ['read', self.datafd, blnum]
        MogamiLog.debug("** read %d block" % (blnum))
        self.r_buflock.acquire()
        self.bldata[blnum].state = 1
        self.r_buflock.release()

        MogamiLog.debug("request to data server %d block" % (blnum))
        with self.plock:
            channel.send_header(cPickle.dumps(senddata), self.psock)
        return 0

    def request_prefetch(self, blnum_list):
        """send request of prefetch.

        @param blnum_list the list of block numbers to prefetch
        """
        MogamiLog.debug("** prefetch ** required blocks = %s" % 
                        (str(blnum_list)))
        # send request to data server
        self.d_channel.prefetch_req(self.datafd, blnum_list)

        with self.r_buflock:
            for blnum in blnum_list:
                self.bldata[blnum].state = 1

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
    def __init__(self, path, size, data_path, flag, *mode):
        MogamiFile.__init__(self, path, size)
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
