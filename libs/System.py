#! /usr/bin/env python
#-*- coding: utf-8 -*-

import os
import os.path
from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)

import logging, threading
import time

class MogamiDaemons(threading.Thread):
    def __init__(self, ):
        threading.Thread.__init__(self)
        self.setDaemon(True)

class MogamiThreadCollector(MogamiDaemons):
    """Collect dead threads in arguments.
    
    @param daemons thread list to collect: alive and dead threads are included
    """
    def __init__(self, daemons):
        MogamiDaemons.__init__(self)
        self.daemons = daemons   # list of daemons

    def run(self, ):
        while True:
            daemons_alive = threading.enumerate()
            for d in self.daemons:
                if d not in daemons_alive:
                    d.join()
                    MogamiLog.debug("** join thread **")
                    self.daemons.remove(d)
            time.sleep(3)

class MogamiPrefetchTread(MogamiDaemons):
    """
    """
    def __init__(self, par):
        MogamiDaemons.__init__(self)
        self.sock = par.psock
        self.bldata = par.mogami_file.bldata
        self.r_buflock = par.mogami_file.r_buflock
        self.plock = par.mogami_file.plock
        self.par = par
        self.rtt = par.rtt

        MogamiLog.debug("** [prefetch thread] init OK")

    def run(self, ):
        pre_num_change = False
        time_list = []
        while True:
            readable = select.select([self.sock.fileno()], [], [], 0)
            if len(readable[0]) == 0:
                pre_num_change = True
            else:
                pre_num_change = False
            select.select([self.sock.fileno()], [], [])
            buf = channel.recvall(self.sock, conf.bufsize)
            if len(buf) != conf.bufsize:
                MogamiLog.debug("break prefetch thread's loop")
                self.bldata = None
                break

            l = cPickle.loads(buf)
            errno = l[0]
            blnum = l[1]
            size = l[2]

            if size == 0:
                with self.r_buflock:
                    self.bldata[blnum].state = 2
                    self.bldata[blnum].buf = ""
                continue

            select.select([self.sock.fileno()], [], [])
            
            (buf, recv_time) = channel.recvall_with_time(self.sock, size)

            if conf.prefetch == True:
                time_list.append(recv_time)
                if len(time_list) > 5:
                    time_list.pop(0)
                    eval_time = 0
                for t in time_list:
                    eval_time += t
                    eval_time /= len(time_list)
                if len(buf) != size:
                    MogamiLog.debug("break prefetch thread's loop")
                    self.bldata = None
                    break
                if pre_num_change == True:
                    recv_size = size / float(1024) / float(1024)
                    self.par.prenum = int(recv_size / eval_time * self.rtt)
                    self.par.prenum += 1
                    MogamiLog.debug("prenum is changed to %d" %
                                    (self.par.prenum))
                    MogamiLog.debug("time = %f, eval_time = %f" %
                                    (recv_time, eval_time))

                    MogamiLog.debug("prefetch recv %d byte bnum %d" %
                                    (len(buf), blnum))
                if self.bldata != None:
                    with self.r_buflock:
                        self.bldata[blnum].state = 2
                        self.bldata[blnum].buf = buf


class Singleton(type):
    """Singleton Class implementation from
    http://code.activestate.com/recipes/412551/
    """

    def __init__(self, *args):
        type.__init__(self, *args)
        self._instance = None

    def __call__(self, *args):
        if self._instance is None :
            self._instance = type.__call__(self, *args)
        return self._instance


class MogamiLog(object):
    """Mogami Logger Class.
    """
    __metaclass__ = Singleton
    
    # Type of Component
    FS = 0
    META = 1
    DATA = 2
    SCHEDULER = 3

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    
    def __init__(self, *args):
        """
        >>> i1 = MogamiLog()
        >>> i2 = MogamiLog()
        >>> assert(i1 == i2)
        """
        pass

    @staticmethod
    def init(log_type, output_level):
        """Initialize logger.

        @param log_type
        @param output_level
        >>> MogamiLog.init("meta", MogamiLog.DEBUG)
        """
        instance = MogamiLog()

        logdir = os.path.join(os.path.dirname(__file__), "..", "log")
        if log_type == "fs":
            instance.logfile = os.path.join(logdir, "mogami.log")
        elif log_type == "meta":
            instance.logfile = os.path.join(logdir, "meta.log")
        elif log_type == "data":
            instance.logfile = os.path.join(logdir, "data.log")
        elif log_type == MogamiLog.SCHEDULER:
            instance.logfile = os.path.join(logdir, "scheduler.log")
        else:
            raise
        if os.access(logdir, os.W_OK) == False:
            print "Directory for log is permitted to write."
            raise Exception
        logging.basicConfig(filename=instance.logfile, 
                            level=output_level,
                            format='[%(asctime)s] %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Logging Start")    

    @staticmethod
    def debug(msg):
        logging.debug(msg)
    
    @staticmethod
    def info(msg):
        logging.info(msg)
    
    @staticmethod
    def warning(msg):
        logging.warning(msg)

    @staticmethod
    def error(msg):
        logging.error(msg)

    @staticmethod
    def critical(msg):
        logging.critical(msg)

def usagestr():
    """Usage string.
    """
    return ""+ fuse.Fuse.fusage


if __name__ == "__main__":
    import doctest
    doctest.testmod()
