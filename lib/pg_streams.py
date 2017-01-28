#!/usr/bin/python3 -uB
import guc
import sys
import logging
import ipc_streams
import enum
import select


import pg_messages

LOGGER = logging.getLogger(__name__)



DEFAULT_CHUNK_SIZE = 16384


class PgBackendState(enum.Enum):
	"""
		These are possible states a postgres backend,
		and by extension a pgplex DownStreamSession, can be in.
		With the exception of states pertaining the initialization sequence,
		and the "busy" states
	"""
	
	# this happens only at startup
	WaitingForUnqualifiedMessage = None


class DownStreamSession(ipc_streams.Stream):
	"""
		Handler of the process + connection talking to the actual frontend and
		to its backend-facing counterpart
	"""
	def __init__(self,
		ds_sock,
		outbound = None
	):


		# upstream connection, if there is any
		self.backend_stream = None


		# if we're currently handling a message that is targeted at the client,
		# it's here
		downstream_message = None

		# here we store the remainder of the data
		downstream_buffer = None


		# pool-facing counterpart of upstream message
		upstream_message = None



		# downstreams sessions are always inbound
		super().__init__(ds_sock = ds_sock, outbound = False)

		for in_buf in iter(self.pop_next_chunk, b""):
			self.connection.send(in_buf)


	def pop_next_chunk(self, size = DEFAULT_CHUNK_SIZE, timeout = None):
		"""
			Waits for either end (the pool or the client) to send some data and
			returns it

			Args:
				suize:		(int)How many bytes to fetch
				timeout:	(float)How many seconds to wait before returning empty-handed

			Returns a 2-tuple:
				- a pg_messages.PeerTypes representing where the chunk came from
				- the chunk data (can obviously be 0)
		"""
		readers = [self.connection]
		if (self.backend_stream is not None):
			readers.append(self.backend_stream.connection)
		
		events = select.select(readers, [], [], timeout)
		if (len(events[0])):
			return events[0][0].recv(size)
		
		
		


	def pop_next_message(self, timeout = None, forward = False):
		"""
			Message collector, forwarder.
			
			Blocks until it managed to buffer the entirety of the next message,
			or the timeout expired.
			The message may be coming from a pool member (chunks coming from shared memory,
			based on addresses sent over the synchronisation socket)

			Or from the client socket.

			For larger messages it'd make sense to allow for chunked forwarding in order
			to mitigate serialization delay problems and memory blowouts
		"""
	
