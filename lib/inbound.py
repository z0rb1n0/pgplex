#!/usr/bin/python -uB
import guc
import socket
import logging


import ipaddress

LOGGER = logging.getLogger(__name__)


class Listener(object):
	"""
		The dispatcher for inbound connections.
		
		Sets up the listening sockets and then returns one connection at a time
		via its workhorse method get_next_session()
	"""
	
	socket = None
	
	bin_addresses = []


	def setup(self, addresses, ports):
		pass
