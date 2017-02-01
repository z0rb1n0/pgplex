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

import info
import guc
import psycopg2

LOGGER = logging.getLogger(__name__)


cnn_str = "host=localhost dbname=pgplex user=bleh password=yolo sslmode=require"


conn = psycopg2.connect(cnn_str)
