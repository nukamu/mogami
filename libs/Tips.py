#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

import sys, os
sys.path.append(os.pardir)
import threading, time

from conf import conf


class MogamiFileAccessPattern(object):
    """This is the class to manage the access pattern of a file.
    """
    MOD_SEQ = 0
    MOD_STRIDE = 1
    MOD_RAND = 2

    def __init__(self, ):
        """
        >>> access_pattern = AccessPattern() 
        """
        self.mode = AccessPattern.MOD_SEQ
        self.stride_read_size = 0
        self.stride_seek_size = 0
        self.lock = threading.Lock()
        self.last_read = 0
        self.last_bl = 0
        self.last_seq_read_start = 0

    def change_mode(self, mode, args=[]):
        """
        >>> access_pattern.change_mode(AccessPattern.MOD_STRIDE, (16, 16))
        """
        with self.lock:
            self.mode = mode
            if len(args) == 0:
                return
            self.stride_read_size = args[0]
            self.stride_seek_size = args[1]

    def tell_mode(self, ):
        return self.mode

    def check_mode(self, offset, size):
        with self.lock:
            if self.mode == AccessPattern.MOD_SEQ:
                if offset == self.last_read:
                    #print "[tips] mode sequential"
                    return
                elif offset < self.last_read:
                    #print "[tips] mode rand"
                    self.mode = AccessPattern.MOD_RAND
                else:
                    #print "[tips] mode stride"
                    self.mode = AccessPattern.MOD_STRIDE
                    self.stride_read_size = self.last_read - self.last_seq_read_start
                    self.stride_seek_size = offset - self.last_read
            # 現在ストライドアクセスのとき
            elif self.mode == AccessPattern.MOD_STRIDE:
                if offset == self.last_read:
                    # 続きがリクエストされたとき
                    if offset + size - self.last_seq_read_start > self.stride_read_size:
                        self.mode = AccessPattern.MOD_SEQ
                        #print "[tips] mode sequential"
                    else:
                        #print "[tips] mode stride"
                        return
                else:
                    if (self.last_read - self.last_seq_read_start == self.stride_read_size) and (offset - self.last_read == self.stride_seek_size) and (size <= self.stride_read_size):
                        #print "[tips] mode stride"
                        return
                    else:
                        #print "[tips] mode rand"
                        self.mode = AccessPattern.MOD_RAND
                    
            else:
                if offset == self.last_read:
                    #print "[tips] mode sequential"
                    self.mode = AccessPattern.MOD_SEQ


    def change_info(self, offset, res_size, last_bl):
        if self.last_read != offset:
            self.last_seq_read_start = offset
        self.last_read = offset + res_size
        self.last_bl = last_bl

    def return_need_block(self, last_read, bl_num, max_bl_num):
        """
        """
        ret_block_list = []
        start_t = time.time()
        start_bl = last_read / conf.blsize
        if last_read % conf.blsize == 0 and last_read != 0:
            start_bl -= 1

        with self.lock:
            if self.mode == AccessPattern.MOD_SEQ:
                if start_bl + 1 + bl_num > max_bl_num:
                    ret_block_list = range(start_bl + 1, max_bl_num)
                else:
                    ret_block_list = range(start_bl + 1, start_bl + 1 + bl_num)
                return ret_block_list
            elif self.mode == AccessPattern.MOD_STRIDE:
                next_offset = last_read + self.stride_seek_size
                if self.stride_read_size + self.stride_seek_size < conf.blsize:
                    if start_bl + 1 + bl_num > max_bl_num:
                        ret_block_list = range(start_bl + 1, max_bl_num)
                    else:
                        ret_block_list = range(start_bl + 1, start_bl + 1 + bl_num)
                    return ret_block_list
                while True:
                    req_list = self.cal_bl_list(next_offset, self.stride_read_size)
                    if req_list[0] > max_bl_num:
                        break
                    for req_bl in req_list:
                        if req_bl not in ret_block_list and start_bl < req_bl:
                            ret_block_list.append(req_bl)
                    if len(ret_block_list) >= bl_num or ret_block_list[-1] >= max_bl_num:
                        break
                    next_offset += self.stride_read_size + self.stride_seek_size
        return ret_block_list[:bl_num]

    def cal_bl_list(self, offset, size):
        blbegin = offset / conf.blsize
        blend = (offset + size - 1) / conf.blsize + 1
        return range(blbegin, blend)

