#!/usr/bin/python3 -uB
import guc
import socket
import logging
import select
import guc
import os
import time
import signal


from ipc_streams import generalize_peer_string
import pg_streams

LOGGER = logging.getLogger(__name__)

UNIX_SOCKET_TEMPLATE = "{directory}/.s.PGSQL.{port}"

class SocketException(Exception):
	"""	Generic socket error """
	pass
class ListenerException(Exception):
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

		
		# this is useful to keep trick of the listener following forks
		self._listener_pid = None

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
		self.socket_objects = {}
		# this is the same list, just flattened (single level, no indication of address family) and indexed by file descriptor number
		# Useful for quick select() and such
		self.socket_fds = {}


		# list of the client-facing children, indexed by PID
		# each member is a 2 tuple containing the remote peer and the origiating listener socket
		self.children = {}


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
						(r_addr if (a_type == socket.AF_UNIX) else (socket.inet_ntop(a_type, r_addr)))
					))

	def __del__(self):
		# we do absolutely nothing unless this is the same process that started it
		if ((self._listener_pid is not None) and (self._listener_pid == os.getpid())):
			self.stop()


	def shutdow_listeners(self):
		"""
			Method to take down all the listeners,
			isolated from stop() so that forked processes can call it
		"""
		for bound_socket in self.socket_fds.values():
			bound_socket.close()
		self.socket_fds = {}
		self.socket_objects = {}

	# we do what we can to shut down all the already bound sockets
	def stop(self, kill_children = False):
		"""
			Takes down all the listeners sockets. And eventually the offspring
			
			Args:
				kill_children:		(bool)Terminate all the children this listener spawned
			
		"""
		
		self.shutdow_listeners()

		if (signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL):
			signal.signal(signal.SIGCHLD, signal.SIG_DFL)

		# just for good measure
		self.reap_children()

		# this loop is still required as the children hold a reference to the sockets that spawned them
		for cli_pid in dict(self.children):
			if (kill_children):
				for next_sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
					os.kill(cli_pid, next_sig)
					# we don't account for hanging processes as there isn't much we can do
					del(self.children[cli_pid])
					time.sleep(0.1)
			else:
				# clean up that reference
				self.children[cli_pid][1] = None

		self._listener_pid = None


	def start(self, addresses = ()):
		"""
			Initializes al necessary sockets and essentially prepares the listener
			for accept()s.
			
			Returns None or fails fatally if one any of the bind() or listen() calls fails.
			Tries to clean up after it self on failure

			Args:
				addresses:		(iterable)Passed as-is to add_bind_targets()
		"""
		
		self._listener_pid = os.getpid()
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

				if (not (af in self.socket_objects)):
					self.socket_objects[af] = {}
				self.socket_objects[af][raw_addr] = ns
				self.socket_fds[ns.fileno()] = ns

				# unfortunately some gymnastics are needed to check for collisions with
				# existing unix sockets
				if (af == socket.AF_UNIX):
					if (os.path.exists(bind_addr)):
						# we try to connect to it. If it fails, we delete the entry
						ts = socket.socket(family = socket.AF_UNIX, type = socket.SOCK_STREAM)
						try:
							ts.connect(bind_addr)
							ts.shutdown(socket.SHUT_RD | socket.SHUT_WR)
							ts.close()
							del(ts)
						except ConnectionRefusedError as e_cnn:
							LOGGER.info("Stale, unconnectible socket `%s` detected. Removing", bind_addr_str)
							os.unlink(bind_addr)

				try:
					

					self.socket_objects[af][raw_addr].bind(bind_addr)
					# add socket to dictionary

					LOGGER.debug("Bound new socket to `%s`" % (bind_addr_str,))
				except Exception as e_bind:
					self.stop()
					raise SocketException("Could not bind socket to address `%s`" % (bind_addr_str,)) from e_bind

				try:
					self.socket_objects[af][raw_addr].listen()
					LOGGER.debug("Socket bound to `%s` is now listening" % (bind_addr_str,))
				except Exception as e_listen:
					self.stop()
					raise SocketException("Could listen on socket bound to `%s`" % (bind_addr_str,)) from e_listen

		# TIS vital!!!
		signal.signal(signal.SIGCHLD, self.signal_handler)



	def reap_children(self):
		"""
			This is both a signal handler and a workaround for pythons
			habit of missing SIGCHLD. We just keep looping until there are no
			more zombies
			
			
			Returns a list of pids that were reaped
		"""
		
		
		# We should be using waitpid() with WNOHANG, but apparently, unlike what
		# the docs say, it makes no difference as a python exception is raised
		# anyway
		reaped = (-1, 0)

		so_far = []
		while (reaped[0] != 0):
			try:
				reaped = os.wait()
			except ChildProcessError as e_nochild:
				# we leave the loop
				reaped = (0, 0)

			if (reaped[0] > 0):
				LOGGER.info("Client session with PID %d(client: %s) ended" % (
					reaped[0], generalize_peer_string(self.children[reaped[0]][0])
				))
				so_far.append(self.children[reaped[0]])
				del(self.children[reaped[0]])

		return so_far



	def signal_handler(self, sig_num, int_frame):
		""" We handle all signals through the same handler """
		if (sig_num == signal.SIGCHLD):
			self.reap_children()


		

	def get_next_client(self, housekeeping_interval = None):
		"""
			Repeatedly select()s over the listener's socket until a connection comes in

			Args:
				housekeeping_interval:		(int)How many milliseconds to select() for in between maintenance loops
				
			Return:
				- In the child thread: a DownStreamSession objects
				- In the parent: a 2-tuple
					- The remote peer, socket.accept()[1]
					- The socket object that accepted this connection
				


		"""
		
		if (not len(self.socket_fds)):
			raise ListenerException("No sockets are bound")
			return None

		real_hki = housekeeping_interval if (housekeeping_interval is not None) else guc.get("housekeeping_interval")

		bound_sockets = self.socket_fds.values()
		readable = ()
		while (not len(readable)):
			(readable, writable, events) = select.select(bound_sockets, (), (), (float(real_hki) / 1000.0) if (real_hki > 0) else None)
			self.reap_children()

		# we only accept a connection at a time
		c_sock = readable[0].accept()
		

		n_pid = os.fork()
		if (n_pid > 0):
			# parent. We maintain our table of children and return
			self.children[n_pid] = [c_sock[1], readable[0]]
			return(self.children[n_pid])
		elif (n_pid == 0):
			signal.signal(signal.SIGCHLD, signal.SIG_DFL)
			# child. We stop the listener and return the session
			self.stop()
			# if it's a unix socket, this is a little hack...
			return pg_streams.DownStreamSession(ds_sock = c_sock[0])
		else:
			raise Exception("Got a negative PID from python's fork. LOL")
