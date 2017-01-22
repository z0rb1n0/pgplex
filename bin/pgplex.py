#!/usr/bin/python3 -uB

import sys
import os
# we use standard unix-style paths as opposed to Python's directory hierarchy
bp = os.path.realpath(os.path.dirname(__file__) + "/../lib")
if (not (bp in sys.path)):
	# lib goes after the script's own path
	sys.path.insert(1 if len(sys.path) else 0, bp)
del(bp)

import info
import config
from config import guc as pgplex_guc
import args




