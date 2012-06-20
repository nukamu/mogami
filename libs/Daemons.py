#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import os
import sys
sys.path.append(os.pardir)

from conf import conf
from System import MogamiLog
import threading
import select
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

class MogamiPrefetchThread(MogamiDaemons):
    """Class for the thread to recv data.
    """
    def __init__(self, mogami_file):
        MogamiDaemons.__init__(self)
        self.mogami_file = mogami_file
        self.p_channel = mogami_file.p_channel
        MogamiLog.debug("** [prefetch thread] init OK")

    def run(self, ):
        pre_num_change = False
        time_list = []
        while True:
            readable = select.select(
                [self.p_channel.sock.fileno()], [], [], 0)
            if len(readable[0]) == 0:
                pre_num_change = True
            else:
                pre_num_change = False

            select.select([self.p_channel.sock.fileno()], [], [])

            header = self.p_channel.recv_msg()
            if header == None:
                MogamiLog.debug("break prefetch thread's loop")
                self.mogami_file.r_data = None
                break

            errno = header[0]
            blnum = header[1]
            size = header[2]

            if size == 0:
                with self.mogami_file.r_buflock:
                    self.mogami_file.r_data[blnum].state = 2
                    self.mogami_file.r_data[blnum].buf = ""
                continue

            select.select([self.p_channel.sock.fileno()], [], [])
            
            (buf, recv_time) = self.p_channel.recvall_with_time(size)

            if conf.prefetch == True:
                time_list.append(recv_time)
                eval_time = 0
                if len(time_list) > 5:
                    time_list.pop(0)
                for t in time_list:
                    eval_time += t
                    eval_time /= len(time_list)
                if len(buf) != size:
                    MogamiLog.debug("break prefetch thread's loop")
                    self.mogami_file.r_data = None
                    break
                if pre_num_change == True:
                    recv_size = size / float(1024) / float(1024)
                    self.mogami_file.prenum = int(recv_size / eval_time *
                                          self.mogami_file.rtt)
                    self.mogami_file.prenum += 1
                    MogamiLog.debug("prenum is changed to %d" %
                                    (self.mogami_file.prenum))
                    MogamiLog.debug("time = %f, eval_time = %f" %
                                    (recv_time, eval_time))

                    MogamiLog.debug("prefetch recv %d byte bnum %d" %
                                    (len(buf), blnum))
            if self.mogami_file.r_data != None:
                with self.mogami_file.r_buflock:
                    self.mogami_file.r_data[blnum].state = 2
                    self.mogami_file.r_data[blnum].buf = buf


