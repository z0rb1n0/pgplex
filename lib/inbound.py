#!/usr/bin/python3 -uB
import guc
import socket
import logging
import select
import guc
import os

LOGGER = logging.getLogger(__name__)


class SocketException(Exception):
	"""	Generic socket error """
	pass
class DNSException(Exception):
	"""	Generic DNS error"""
	pass

class Listener(object):
	"""
		The dispatcher for inbound connections.
		
		Sets up the listening sockets and then returns one connection at a time
		via its workhorse method get_next_session()
		
		
		NOTE: In order to keep things consistent with the way postgres binding
		works, it is not possible to bind different socket pairs on different
		ports
	"""
	


	bind_port = None
	
	# This dictionary is indexed by post-deduplication IPV4/6 address (packed). Each
	# member is a list of the host names (passed to to add_bind_targets()) that resolved into
	# the indexed address
	bind_addresses = {}


	# socket objects. Indexed by packed bind address to easily identify what's left to clean up
	sockets = {}


	def __init__(self,
		port = 5432,
		addresses = (),
		setup_on_init = False
	):
		"""
			Allows one to initialize[, load[ and bind]] the listener
			
			Fails fatally

			Args:
				port:			(int)Sets the listener port. Applies to all bound sockets
				addresses:		(iterable)Fed as-is to add_bind_targets().
		"""
		
		self.bind_port = port
		if (setup_on_init):
			self.setup(addresses)
		else:
			self.add_bind_targets(addresses)


	def add_bind_targets(self, addresses):
		"""
			Resolves the supplied host names and populates bind_addresses accordingly.
			
			Returns None or fails fatally
			
			Args:
				addresses:		(iterable)Host names/ip addresses. They are deduplicated post name-resolution. Accepts a scalar too
		"""
		for new_address in (addresses if (isinstance(addresses, (tuple, list))) else (addresses,)):
			LOGGER.debug("Resolving `%s` for listener" % (new_address,))
			try:
				addr_info = socket.getaddrinfo(host = new_address, port = None)
			except Exception as e_ns:
				raise DNSException("Unable to resolve `%s`" % (new_address)) from e_ns


			# we index by the packed ip address.
			# given that only ipv4 addresses can contain dots, we use that a discriminator for the address family
			usable_addresses = tuple(map(lambda ab : socket.inet_pton((socket.AF_INET if ("." in ab[4][0]) else socket.AF_INET6), ab[4][0]), filter(
				lambda ai : (isinstance(ai[4], (tuple, list)) and ai[4][0]),
				addr_info
			)))

			if (not len(usable_addresses)):
				raise DNSException("`%s` did not resolve to any usable IPV{4/6} address" % (new_address,))

			# time to add these fellas to the known address list
			for r_addr in usable_addresses:
				if (r_addr not in self.bind_addresses):
					self.bind_addresses[r_addr] = []

				if (new_address not in self.bind_addresses[r_addr]):
					self.bind_addresses[r_addr].append(new_address)
					LOGGER.debug("`%s` resolved to `%s`" % (new_address, socket.inet_ntop(socket.AF_INET6 if (len(r_addr) > 4) else socket.AF_INET, r_addr)))

	# we do what we can to shut down all the already bound sockets
	def shutdown_listeners(self):
		"""
			Takes down all the listeners sockets.
		"""
		for (sock_addr, sock_obj) in dict(self.sockets.items()).items():
			sock_obj.close()
			del(self.sockets[sock_addr])

	def setup(self, addresses = ()):
		"""
			Initializes al necessary sockets and essentially prepares the listener
			for accept()s.
			
			Returns None or fails fatally if one any of the bind() or listen() calls fails.
			Tries to clean up after it self on failure

			Args:
				addresses:		(iterable)Passed as-is to add_bind_targets()
		"""
		
		self.add_bind_targets(addresses)


		
		for r_addr in self.bind_addresses:
			# we determine the address type in a really naive way
			af = (socket.AF_INET6 if (len(r_addr) > 4) else socket.AF_INET)
			addr_p = socket.inet_ntop(af, r_addr)
			s_pair = (addr_p, self.bind_port)
			if (len(r_addr) > 4):
				# not completely sure about link-local being a wise choice here
				# No handling of flows yet
				s_pair += (0, 0x2)

			# we don't catch errors for these l
			ns = None
			ns = socket.socket(family = af, type = socket.SOCK_STREAM)
			ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			try:
				ns.bind(s_pair)
				self.sockets[r_addr] = ns
				LOGGER.debug("Bound new socket to [%s]:%d" % (addr_p, self.bind_port))
			except Exception as e_bind:
				cleanup_sockets()
				raise SocketException("Could not bind socket to address `[%s]:%d`" % (addr_p, self.bind_port)) from e_bind

			try:
				ns.listen()
				LOGGER.debug("Socket bound to [%s]:%d is now listening" % (addr_p, self.bind_port))
			except Exception as e_listen:
				self.shutdown_listeners()
				raise SocketException("Could listen on socket bound to `[%s]:%d`" % (addr_p, self.bind_port)) from e_listen

	def get_next_client(self, housekeeping_interval = None):
		"""
			select()s over the listener's sockets and yields a Client
			object, representing a freshly established connection
		"""
		real_hki = housekeeping_interval if (housekeeping_interval is not None) else guc.get("housekeeping_interval")
		LOGGER.debug("Waiting for next client. Housekeeping interval is %dms" % (real_hki))

		while True:
			(readable, writable, events) = select.select(self.sockets.values(), (), (), (float(real_hki) / 1000.0) if (real_hki > 0) else None)
			if (len(readable)):
				for ready_socket in readable:
					c_sock = ready_socket.accept()
					n_pid = os.fork()
					if (n_pid > 0):
						# daddy
						print(c_sock[1])
						#LOGGER.info("Received new connection from 
					elif (n_pid == 0):
						# childy
						self.shutdown_listeners()
						# echo service
						while True:
							c_sock[0].send(c_sock[0].recv(512))
					else:
						# need to handle this
						raise Exception("Fork failed...")
