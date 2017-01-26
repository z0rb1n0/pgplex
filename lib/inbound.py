#!/usr/bin/python3 -uB
import guc
import socket
import logging
import select
import guc
import os
import time

LOGGER = logging.getLogger(__name__)

UNIX_SOCKET_TEMPLATE = "{directory}/.s.PGSQL.{port}"

class SocketException(Exception):
	"""	Generic socket error """
	pass
class DNSException(Exception):
	"""	Generic DNS error"""
	pass



def generalize_peer_string(peer_str):
	"""
		Different address families have different structures.
		This function accepts them all and tries to come up with a reasonable
		string representation
		
		Args:
			address, as the socket library handles it (could be a 2-tuple, string, w/e)
			
		Return:
			a string, typically an RFC compliant string representation of an address
	"""
	if (isinstance(peer_str, (str, bytes))):
		# empty AF_UNIX peer addresses are better represented as a generic "[local]" string
		return str(peer_str) if len(peer_str) else "[local]"
	else:
		# wrap IPV6 retardedness in square brackets
		is_ipv6 = (not ("." in peer_str[0]))
		return "%s%s%s:%d" % (
			("[" if (is_ipv6) else ""),
			peer_str[0],
			("]" if (is_ipv6) else ""),
			peer_str[1]
		)


class DownStream(object):
	"""
		Handler of the process + connection talking to the actual frontend.
	"""
	def __init__(
		self,
		ds_sock
	):
		"""
			The constructor only ensures that the socket is a socket
		"""
		if ((not isinstance(ds_sock, (socket.socket,))) or (ds_sock.fileno() < 0)):
			raise SocketException("Invalid/unconnected socket")


		# the actual data stream
		self.stream = ds_sock


		print("Hello, I'm " + self.local_peer + ", serving " + self.remote_peer)
		while True:
			ds_sock.send(ds_sock.recv(512))

	@property
	def local_peer(self):
		""" Convenience property. Returns a string representation """
		return generalize_peer_string(self.stream.getsockname())

	@property
	def remote_peer(self):
		""" Convenience property. Returns a string representation """
		return generalize_peer_string(self.stream.getpeername())



class Listener(object):
	"""
		The dispatcher for inbound connections.
		
		Sets up the listening sockets and then returns one connection at a time
		via its workhorse method get_next_session()

		
		NOTE: In order to keep things consistent with the way postgres socket binding
		works, it is not possible to bind different socket pairs on different
		ports
	"""
	


	def __init__(self,
		port = 5432,
		addresses = (),
		unix_socket_dirs = (),
		start_on_init = False
	):
		"""
			Allows one to initialize[, load[ and bind]] the listener
			
			Fails fatally

			Args:
				port:			(int)Sets the listener port. Applies to all bound sockets
				addresses:		(iterable)Fed as-is to add_bind_targets().
		"""

		# The port we will bind to
		self.bind_port = port

		# This dictionary is the list of addresses that we intend to bind to.
		# It is broken into 2 levels due to a possible collision between a packed IP address
		# and a canonicalized file system path. Any given member is addressed as follows
		# bind_addresses[socket.AF_*][packed_address_or_path]
		# 
		# Each member contains a list of the aliases that the packed address/canonical path
		# was resolved/canonicalized from
		self.bind_addresses = {}


		# socket objects that are meant to listen. Indexed by [address_family][packed bind address/path] to easily identify what's left to clean up
		self.sockets = {}
		
		
		# list of the client-facing children, indexed by PID
		self.downstreams = {}


		if (start_on_init):
			self.start(addresses)
		else:
			self.add_bind_targets(addresses)


	def add_bind_targets(self, addresses):
		"""
			Resolves the supplied path/host names and populates bind_addresses accordingly.
			Operates for both unix and TCP sockets (note that for unix sockets it expects a directory,
			as the socket basename is fixed)

			Returns None or fails fatally
			
			Args:
				addresses:		(iterable)Socket paths/Host names/ip addresses. They are deduplicated post name-resolution. Accepts a scalar too
		"""

		# we treat all sockets the same way, local or not
		
		for new_address in (addresses if (isinstance(addresses, (tuple, list))) else (addresses,)):

			usable_addresses = []
			# for unix socket, we canonicalize the path, for inet, it's standard resolution instead
			if (len(new_address) and (new_address[0] == "/")):
				socket_path = UNIX_SOCKET_TEMPLATE.format(directory = new_address, port = self.bind_port)
				LOGGER.debug("Canonicalizing socket path `%s`" % (new_address,))

				usable_addresses = [ (socket.AF_UNIX, os.path.realpath(socket_path)), ]

			else:

				LOGGER.debug("Resolving listener address `%s`" % (new_address,))
				try:
					addr_info = socket.getaddrinfo(
						host = new_address,
						port = self.bind_port,
						type = socket.SOCK_STREAM,
						proto = socket.IPPROTO_TCP
					)
				except Exception as e_ns:
					raise DNSException("Unable to resolve `%s`" % (new_address)) from e_ns

				# we filter the results and add to the list of "usable addresses"
				for usable_address in filter(
					lambda ai : (isinstance(ai[4], (tuple, list)) and ai[4][0]),
					addr_info
				):
					# here's where we pack the address
					usable_addresses.append((usable_address[0], socket.inet_pton(usable_address[0], usable_address[4][0])))

				if (not len(usable_addresses)):
					raise DNSException("`%s` did not resolve to any usable IPV{4/6} address" % (new_address,))


			# time to add these to the known address list
			for (a_type, r_addr) in usable_addresses:

				if (not (a_type in self.bind_addresses)):
					self.bind_addresses[a_type] = {}

				if (not (r_addr in self.bind_addresses[a_type])):
					self.bind_addresses[a_type][r_addr] = []

				if (r_addr not in self.bind_addresses[a_type][r_addr]):
					self.bind_addresses[a_type][r_addr].append(new_address)
					LOGGER.debug("`%s` %s to `%s`" % (
						new_address,
						"canonicalized" if (a_type == socket.AF_UNIX) else "resolved",
						r_addr
					))


	# we do what we can to shut down all the already bound sockets
	def stop(self):
		"""
			Takes down all the listeners sockets.
		"""
		for (af, bound_sockets) in self.sockets.items():
			for (raw_addr, bound_socket) in bound_sockets.items():
				bound_socket.close()
				
		self.sockets = {}

	def start(self, addresses = ()):
		"""
			Initializes al necessary sockets and essentially prepares the listener
			for accept()s.
			
			Returns None or fails fatally if one any of the bind() or listen() calls fails.
			Tries to clean up after it self on failure

			Args:
				addresses:		(iterable)Passed as-is to add_bind_targets()
		"""
		
		self.add_bind_targets(addresses)

		for (af, addresses) in self.bind_addresses.items():
			for (raw_addr, aliases) in addresses.items():

				# we create the bind address based on the family
				if (af == socket.AF_UNIX):
					# simple string for unix
					bind_addr = raw_addr
				else:
					# inet expects arrays
					# ipv4 has 2 members, ipv6 appends 2 more
					read_addr = socket.inet_ntop(af, raw_addr)
					bind_addr = (read_addr, self.bind_port)
					if (af == socket.AF_INET6):
						# not completely sure about link-local being a wise choice here
						# No handling of flows yet
						bind_addr += (0, 0x2)


				# the structure of the visualized address changes based on family
				bind_addr_str = ("%s" % (raw_addr,)) if (af == socket.AF_UNIX) else ("[%s]:%d" % (read_addr, self.bind_port))

				# address ready
				# we don't catch errors for socket creation/options as failure here is very rare...
				ns = None
				ns = socket.socket(family = af, type = socket.SOCK_STREAM)
				ns.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

				if (not (af in self.sockets)):
					self.sockets[af] = {}
				self.sockets[af][raw_addr] = ns

				# unfortunately some gymnastics are needed to check for collisions with
				# existing unix sockets
				if (af == socket.AF_UNIX):
					if (os.path.exists(bind_addr)):
						# we try to connect to it. If it fails, we delete the entry
						ts = socket.socket(family = socket.AF_UNIX, type = socket.SOCK_STREAM)
						try:
							ts.connect(bind_addr)
							ts.shutodwn(socket.SHUT_RD | socket.SHUT_WR)
							ts.close()
							del(ts)
						except ConnectionRefusedError as e_cnn:
							LOGGER.info("Stale, unconnectible socket `%s` detected. Removing", bind_addr_str)
							os.unlink(bind_addr)

				try:
					

					self.sockets[af][raw_addr].bind(bind_addr)
					# add socket to dictionary
					
					LOGGER.debug("Bound new socket to `%s`" % (bind_addr_str,))
				except Exception as e_bind:
					self.stop()
					raise SocketException("Could not bind socket to address `%s`" % (bind_addr_str,)) from e_bind

				try:
					self.sockets[af][raw_addr].listen()
					LOGGER.debug("Socket bound to `%s` is now listening" % (bind_addr_str,))
				except Exception as e_listen:
					self.stop()
					raise SocketException("Could listen on socket bound to `%s`" % (bind_addr_str,)) from e_listen

	def get_next_downstream(self, housekeeping_interval = None):
		"""
			select()s over the listener's sockets and yields a Client
			object, representing a freshly established connection
		"""
		real_hki = housekeeping_interval if (housekeeping_interval is not None) else guc.get("housekeeping_interval")
		LOGGER.info("Waiting for clients. Housekeeping interval is %dms" % (real_hki))

		# unfortunately we have to build a list with all the sockets
		l_sockets = []
		for (l_af, l_socks) in self.sockets.items():
			for s_obj in l_socks.values():
				l_sockets.append(s_obj)

		while True:
			
			(readable, writable, events) = select.select(l_sockets, (), (), (float(real_hki) / 1000.0) if (real_hki > 0) else None)
			if (len(readable)):
				for ready_socket in readable:
					c_sock = ready_socket.accept()

					local_sock = c_sock[0].getsockname()
					remote_sock = c_sock[0].getpeername()
					# again, some conditional string formatting is required due to different socket types
					connection_vector_str = ""
					if (isinstance(local_sock, (str, bytes))):
						# unix
						local_addr_str = str(local_sock)
						connection_vector_str += "on "
					else:
						# tcp. The source makes sense only here
						local_addr_str = "[%s]:%d" % (local_sock)
						remote_addr_str = "[%s]:%d" % (remote_sock)
						connection_vector_str += "from %s to " % (remote_addr_str,)


					connection_vector_str += "%s" % (local_addr_str,)
					
					try:
						n_pid = os.fork()
					except Exception as e_fork:
						# need to handle this
						LOGGER.warning("Could not fork child process (%s)" % (connection_vector_str))
						c_sock[0].shutdown(socket.SHUT_RD | socket.SHUT_WR)
						c_sock[0].close()
						del(c_sock)
						continue

					if (n_pid > 0):
						# daddy

						# for now we just store an approximate start time here
						self.downstreams[n_pid] = time.time
						LOGGER.debug("New connection %s dispatched to child PID %d" % (connection_vector_str, n_pid))

					elif (n_pid == 0):
						# kiddo
						
						# this will close all the listening sockets
						self.stop()
						
						# echo service
						ds_h = DownStream(c_sock[0])

