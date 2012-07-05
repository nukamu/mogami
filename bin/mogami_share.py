#! /usr/bin/env python

import os
import sys
import re

class MogamiShare(object):
    def __init__(self, args):
        args.pop(0)
        self.meta_addr = args.pop()
        self.args = args

        # initialization
        self.dataservers = {}   # {ip: data-path}
        self.clients = {}       # {ip: mnt-path}

        self.parse_args()

    def parse_args(self, ):
        for arg in self.args:
            if arg.find(":") == -1:
                sys.exit("Parse Error: This format is not available in arg %s"
                         % (arg))
            (hosts, path) = arg.split(":")
            # some format checks
            if path[0] != "/":
                sys.exit("Format Error: shared path should be absolute path")
            

    def parse_hosts(self, hosts):
        """Parse description of hosts like huscs[[001-005]].

        This method is not used now.
        @param hosts strings like 'huscs[[001-002]]' or huscs000
        @return list of hosts (e.g. ['huscs001', 'huscs002', ...]
        """
        host_re = re.compile("(?P<hostprefix>[^\[]+)\[\[(?P<start_node>\d+)-(?P<end_node>\d+)\]\]")
        m_hosts = host_re.match(hosts)
        if m_hosts == None:
            return [hosts, ]

        ret_list = []
        host_prefix = m_hosts.group('hostprefix')
        num_length = len(m_hosts.group('start_node'))
        
        start_num = int(m_hosts.group('start_node'))
        end_num = int(m_hosts.group('end_node'))

        for i in range(start_num, end_num + 1):
            base_str = '%s%0' + str(num_length) + 'd'
            ret_list.append(base_str % (host_prefix, i))
        
        return ret_list

    def finalize(self, ):
        pass


if __name__ == '__main__':
    MogamiShare(sys.argv)
