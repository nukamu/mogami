import logging

"""Define ports number used.
There are three ports Mogami uses.
(meta port, data port and prefetch port.)
"""
metaport=15806
dataport=15807

"""
"""
bufsize=1024
blsize=1024 * 1024

writelen_max=1024 * 1024

force_prenum=False
prenum=10

prefetch=True
write_local=True

multithreaded=True

fs_loglevel=logging.INFO
meta_loglevel=logging.INFO
data_loglevel=logging.INFO
