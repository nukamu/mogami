import logging

# Define ports number used.
metaport=15806
dataport=15807

# Define buffer size and block size in mogami
bufsize=1024
blsize=1024 * 1024

# Define max length without communication
writelen_max=1024 * 1024

# 
force_prenum=False
prenum=10

prefetch=True
write_local=True

multithreaded=True

# Log level
fs_loglevel=logging.INFO
meta_loglevel=logging.INFO
data_loglevel=logging.INFO

# Get access pattern or not
ap=False
