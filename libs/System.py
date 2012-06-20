#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)

import logging
import os.path


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
