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
import inbound

LOGGER = logging.getLogger(__name__)



log_manager.setup_loggers("DEBUG")


LOGGER.info("%s version %d.%d.%d starting" % (info.APP_TITLE, info.APP_MAJOR, info.APP_MINOR, info.APP_REVISION))
guc.reload()
LOGGER.info("Creating and initializing listeners")


# main listener loop begins
l_main = inbound.Listener(
	addresses = (guc.get("unix_socket_directories") + guc.get("listen_addresses")),
	start_on_init = 1
)


nc = ()
while (isinstance(nc, (tuple, list))):
	nc = l_main.get_next_client()
	#print(type(nc))
