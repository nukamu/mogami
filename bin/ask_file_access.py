#! /usr/bin/env python

import sys,os
sys.path.append(os.pardir)

from libs import Channel

def ask_file_access(pid, path):
    ch = Channel.MogamiChanneltoAskAP(path)
    ap_list = ch.file_access_req(pid)
    return ap_list

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit("Usage: %s [pid]")
    ap_list = ask_file_access(int(sys.argv[1]), "/tmp/mogami_ap")
    print ap_list
