#!/usr/bin/python3 -uB

import sys
import os
# we use standard unix-style paths as opposed to Python's directory hierarchy
bp = os.path.realpath(os.path.dirname(__file__) + "/../lib")
if (not (bp in sys.path)):
	# lib goes after the script's own path
	sys.path.insert(1 if len(sys.path) else 0, bp)
del(bp)

import logging
import log_manager
import time
import posix_ipc

import info
import guc
import psycopg2
import psycopg2.extras


LOGGER = logging.getLogger(__name__)


shm_ps = posix_ipc.SharedMemory(
	"/ciccio",
	flags = posix_ipc.O_CREAT,
	mode = 0o600,
	size = 1048576 * 256,
	read_only = False
)

#time.sleep(10)
