#!/usr/bin/python3 -uB
import guc
import socket
import logging
import os
import enum

LOGGER = logging.getLogger(__name__)


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


class StreamSetupError(Exception):
	"""	Generic socket error """
	pass


class Stream(socket.socket):
	"""
		Generic handler for connections. It's just an embellisher for a socket,
		which allows for easier SSL wrapping and the like.

	"""
	def __init__(self,
		ds_sock: socket.socket,
		outbound: bool = None
	):
		"""
			The constructor first ensures that the socket is a socket.
			If it is, then bootstraps a new session by doing all the things
			that characterize a connection-base session.

			Args:
				ds_sock:	(socket.socket)The stream-based socket to build the stream around
				outbound:	(boolean)This nullable boolean offers an option to specify how the
							         connection was initiated, mostly for logging purposes.
							           None = unspecified, logging will be generic
							           False = outbound, the object was connect()ed
							           True = inbound, this is the result of an accept()
			
		"""
		if (ds_sock.fileno() < 0):
			raise StreamSetupError("Invalid/unconnected socket")

		# this needs to be stored
		self._owner_pid = self.pid

		# the actual data stream
		self.socket_object = ds_sock
		
		# we build dictionary to quickly forward attributes to internal members
		# (generally the socket)
		self._proxied_attributes = {}
		for socket_attr in ("read", "write", "send", "recv"):
			self._proxied_attributes["socket_addr"] = self.socket_object
		

		# this is kinda tricky, especially in charging the order
		peers = (self.local_peer, self.remote_peer)
		LOGGER.info("New %sconnection: %s %s-> %s" % (
			("" if (outbound is None) else (("out" if outbound else "in") + "bound ")),
			peers[0 if (outbound) else 1],
			("<" if (outbound is None) else ""),
			peers[1 if (outbound) else 0],
		))


	@property
	def local_peer(self):
		""" Convenience property. Returns a string representation """
		return generalize_peer_string(self.socket_object.getsockname())

	@property
	def remote_peer(self):
		""" Convenience property. Returns a string representation """
		return generalize_peer_string(self.socket_object.getpeername())

	@property
	def pid(self):
		""" Convenience property. Returns the ACTUAL, CURRENT pid """
		return os.getpid()

	@property
	def owner_pid(self):
		"""
			Returns the PID that this session originally spawned, regardless of the call
			running inside subsequent children
		"""
		return self._owner_pid

	@property
	def fileno(self):
		""" Convenience property. Just the file descriptor number of the socket """
		return (self.socket_object.fileno() if self.socket_object else None)


	# we now choose which attributes are simply offloaded to some members of this class
	# (most likely the socket itself)
	def __getattr__(self, attribute):

		try:
			return self._proxied_attributes[attribute]
		except KeyError as ke:
			raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name, attribute))


	def shutdown(self):
		""" Glorified socket shutdown. Always succeeds as it is also used for cleanup """
		if (self.socket_object):
			self.socket_object.shutdown(socket.SHUT_RD | socket.SHUT_WR)
			self.socket_object.close()

		return True

	def __del__(self):
		self.shutdown()

