#!/usr/bin/python3 -uB
import sys
import os
import logging
import ipc_streams
import enum
import select


import guc
import pg_messages

LOGGER = logging.getLogger(__name__)


DEFAULT_BUFFER_SIZE = 16384


class BackEndState(enum.Enum):
	"""
		These are possible states a postgres backend,
		and by extension a pgplex DownStreamSession, can be in.
		With the exception of states pertaining the initialization sequence,
		and the "busy" states


		Each state contains a tuple of the message classes that are accepted in that state.
		
		NOTE: subclasses of the specified class are searched for too, which might pose a performance
		      problem with deep hierarchies of message classes. Might be useful to pre-flatten the
		      structure during module initialization if that happens
		
	"""

	# the following only happen only at startup
	WaitingForInitialMessage = (pg_messages.InitialMessage,)



class DownStreamSession(ipc_streams.Stream):
	"""
		Handler of the process + connection talking to the actual frontend and
		to its backend-facing counterpart
		
		
		NOTE:	inboxes always contain data buffers (perhaps empty), out boxes
				always contain message objects (if any)
			
	"""
	def __init__(self,
		ds_sock,
		outbound = None
	):


		# upstream connection, if there is any
		self.backend_stream = None
		
		# state begs the "what next?" question
		self.state = BackEndState.WaitingForInitialMessage


		# The dictionary containing the input/output buffers, and other information
		# for either side. It's indexed by peer type
		# 0: input (as in: data that hasn't been consumed by a message yet)
		# 1: output
		# 2: stats
		self.mail_boxes = {
			pg_messages.PeerTypes.FrontEnd: [ b"", b"", [] ],
			pg_messages.PeerTypes.BackEnd: [ b"", b"", [] ]
		}


		# downstreams sessions are always inbound
		super().__init__(ds_sock = ds_sock, outbound = False)
		


	def pop_next_chunk(self,
		size = DEFAULT_BUFFER_SIZE,
		timeout = None,
		append_to_inbox = False
	):
		"""
			Waits for either end (the pool or the client) to send some data and
			returns it. Can optionally also append the data to the relevant inbox

			Args:
				size:				(int)How many bytes to fetch
				timeout:			(float)How many seconds to wait before returning empty-handed
				append_to_inbox:	(bool)Whether or not the received data should be added to the relevant inbox

			Returns a 3 tuple as follows:
				- a pg_messages.PeerTypes representing where the chunk came from. Can be Neither if the operation timed out
				- the appended data. None for timeouts or if the read returned no bytes/failed

		"""
		readers = [self.connection]
		if (self.backend_stream is not None):
			readers.append(self.backend_stream.connection)
		
		events = select.select(readers, [], [], timeout)
		if (len(events[0])):


			new_buf = events[0][0].recv(size)

			# which socket did we receive the chunk/event on?
			origin = pg_messages.PeerTypes.FrontEnd if (events[0][0] is readers[0]) else pg_messages.PeerTypes.BackEnd
			if (len(new_buf)):
				self.mail_boxes[origin][0] += new_buf

			return (origin, new_buf if len(new_buf) else None)

		else:
			return (pg_messages.PeerTypes.Neither, None)



	def pop_next_message(self,
		timeout = None
	):
		"""
			Message collector and (hopefully, one day, buffer forwarder too)

			Blocks until it managed to buffer the entirety of the next message,
			or the timeout expired.

			The message may be coming from a pool member (chunks coming from shared memory,
			based on addresses sent over the synchronisation socket) or from the client socket.

			Args:
				timeout:				(float)How long to wait, in seconds


			Returns a 2-tuple:
				- a pg_messages.PeerTypes representing where the message came from. Can be Neither if the operation timed out
				- the message object. None for errors/timeouts


			PLANNED FEATURE:

			For larger messages it'd make sense to allow for stream chunked forwarding in order
			to mitigate serialization delay problems and memory blowouts.
		"""


		# we just keep getting the next chunk until either inbox has a complete message in it

		# here we'll store the output messages,
		# indexed by recepient peertype
		msgs = {}
		while (True):


			# Does any inbox already contain any leftover data?
			# If there is no corresponding message yet, that leftover data needs to be
			# at least as big as the message signature, or we'll leave it in the inbox
			# 
			# If a message is already initialized, we only take the inbox data if it's enough to
			# cover missing_bytes
			for box in self.mail_boxes:
				consumed = 0
				inbox_bc = len(self.mail_boxes[box][0])
				if (inbox_bc):
					if (box not in msgs):
						# Generate an new message
						# there is ONE special case..
						if ((box is pg_messages.PeerTypes.FrontEnd) and (self.state is BackEndState.WaitingForInitialMessage)):
							msg_base_class = pg_messages.InitialMessage
						else:
							msg_base_class = pg_messages.QualifiedMessage

						if (inbox_bc >= msg_base_class.SIGNATURE_SIZE):
							msgs[box] = msg_base_class(self.mail_boxes[box][0])
							consumed = len(msgs[box].data)
					else:
						# the message already knows how much data it needs,
						# however we can't assume it is all available
						consumed = min(inbox_bc, msgs[box].missing_bytes)
						if (inbox_bc >= msgs[box].missing_bytes):
							msgs[box].append(mail_boxes[box][0][0:consumed])


					# it is now time to remove the consumed data from the buffer and
					# if this turns into a speed issue, ctypes/bytearrays could help
					if (consumed):
						self.mail_boxes[box][0] = self.mail_boxes[box][0][consumed:]

					# we return the message if it's complete...
					if (not msgs[box].missing_bytes):
						LOGGER.debug("Received message: %s(%d bytes)", msgs[box].__class__.__name__, len(msgs[box].data))
						return (box, msgs[box])


			# if we didn't get any complete message, it's time to buffer up more data
			nm = self.pop_next_chunk(append_to_inbox = True)
			if ((nm[0] is pg_messages.PeerTypes.Neither) or (nm[1] is None)):
				# buffering failed...
				# it is convenient to have the same message format...
				return (nm)

		raise Exception("Flow should never have reached this point")



	def handle_session(self):
		"""
			This function blocks while the whole protocol handling is running
		"""

		while (True):

			nm = self.pop_next_message()
			if ((nm[0] is None) or (nm[1] is None)):
				LOGGER.error("Read error")
				sys.exit(0)
			print(nm[1])
			#self.connection.send(str(nm[1]))
