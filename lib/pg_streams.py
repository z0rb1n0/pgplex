#!/usr/bin/python3 -uB
import sys
import os
import logging
import ipc_streams
import enum
import select
import ssl


import guc
import pg_messages

LOGGER = logging.getLogger(__name__)


DEFAULT_BUFFER_SIZE = 16384









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
		self.state = pg_messages.SessionState.WaitingForInitialMessage


		# The dictionary containing the input/output buffers, and other information
		# for either side. It's indexed by peer type
		# 0: input (as in: data that hasn't been consumed by a message yet)
		# 1: output
		# 2: stats
		self.mail_boxes = {
			pg_messages.PeerType.FrontEnd: [ b"", b"", [] ],
			pg_messages.PeerType.BackEnd: [ b"", b"", [] ]
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
				- a pg_messages.PeerType representing where the chunk came from. Can be None if the operation timed out
				- the appended data. None for read errors/EOT
				
			Returns None if it times out

		"""
		readers = [self.connection]
		if (self.backend_stream is not None):
			readers.append(self.backend_stream.connection)
		
		events = select.select(readers, [], [], timeout)
		if (len(events[0])):


			new_buf = events[0][0].recv(size)

			
			#LOGGER.debug("Received buffer: %s", new_buf)

			# which socket did we receive the chunk/event on?
			origin = pg_messages.PeerType.FrontEnd if (events[0][0] is readers[0]) else pg_messages.PeerType.BackEnd
			if (len(new_buf)):
				self.mail_boxes[origin][0] += new_buf

			return (origin, new_buf if len(new_buf) else None)

		else:
			# nothing came along
			return None



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
				- a pg_messages.PeerType representing where the message came from. Can be None if the operation timed out
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
				#LOGGER.debug("Buffer contents: %d bytes(%s)" % (inbox_bc, self.mail_boxes[box][0]))
				if (inbox_bc):
					if (box not in msgs):
						# Generate an new message
						# there is ONE special case..
						if ((box is pg_messages.PeerType.FrontEnd) and (self.state is pg_messages.SessionState.WaitingForInitialMessage)):
							expected_message_class = pg_messages.InitialMessage
						else:
							expected_message_class = pg_messages.QualifiedMessage


						if (inbox_bc >= expected_message_class.SIGNATURE_SIZE):
							msgs[box] = expected_message_class(self.mail_boxes[box][0])
							consumed = len(msgs[box].data)
					else:
						# the message already knows how much data it needs,
						# however we can't assume it is all available
						consumed = min(inbox_bc, msgs[box].missing_bytes)
						if (inbox_bc >= msgs[box].missing_bytes):
							msgs[box].append(mail_boxes[box][0][0:consumed])


					#LOGGER.debug("Bytes consumed: %d" % consumed)
					# it is now time to remove the consumed data from the buffer and
					# if this turns into a speed issue, ctypes/bytearrays could help
					if (consumed):
						self.mail_boxes[box][0] = self.mail_boxes[box][0][consumed:]

					# we return the message if it's complete...
					if (not msgs[box].missing_bytes):
						LOGGER.debug("Received message from %s: %s. %d bytes left in inbox", box.name, msgs[box], len(self.mail_boxes[box][0]))
						return (box, msgs[box])


			# if we didn't get any complete message, it's time to buffer up more data
			nm = self.pop_next_chunk(timeout = timeout, append_to_inbox = True)
			if ((nm is None) or (nm[1] is None)):
				# buffering failed...
				# it is convenient to have the same message format...
				return (nm)

		raise Exception("Flow should never have reached this point")



	def handle_session(self):
		"""
			This function blocks while the whole protocol handling is running
		"""

		# timeout at first is authentication_timeout
		pop_timeout = guc.get("authentication_timeout")


		while (True):

			popped = self.pop_next_message(pop_timeout)
			
			if (popped is None):
				src_peer = None
				if (self.state is pg_messages.SessionState.WaitingForInitialMessage):
					expected = "Startup message"
				elif (self.state is (pg_messages.SessionState.WaitingForCredentials)):
					expected = "Authentication"
				else:
					# this sould never happen
					expected = "Idle"

				LOGGER.error("%s timeout of %.3fs was exceeded", expected, pop_timeout)
			elif ((popped is not None) and (popped[1] is None)):
				src_peer = None
				LOGGER.error("Connection terminated by peer/socket time out")
			else:
				(src_peer, msg) = (popped[0], popped[1])


			if (src_peer is None):
				# everything went south
				self.shutdown()
				sys.exit(0)



			# time to check if the state is good for this peer type/backend state
			if ((src_peer in msg.VALID_START_STATES) and (self.state in msg.VALID_START_STATES[src_peer])):
				pass
			else:
				LOGGER.error("Invalid message %s for current %s-facing stream in state %s",
					msg.__class__.__name__, src_peer.name, self.state.name
				)
				self.shutdown()
				sys.exit(0)

			if (src_peer is pg_messages.PeerType.FrontEnd):
				# this recursive class-based lookup may be slow. If we want to accelerate it,
				# the classes referenced by pg_messages.QUALIFIED_MESSAGE_TYPE_SIGNATURE_MAPS
				# need to be hierarchically into a dictionary to allow for direct hash-based lookups
				if (isinstance(msg, pg_messages.SSLRequest)):

					ssl_enabled = guc.get("ssl")

					if (ssl_enabled):
						self.connection.send(b"S") # last unencrypted byte
						self.connection = ssl.wrap_socket(self.connection,
							keyfile = guc.get("ssl_key_file"),
							certfile = guc.get("ssl_cert_file"),
							server_side = True,
							cert_reqs = ssl.CERT_NONE,
							ca_certs = guc.get("ssl_ca_file"),
							do_handshake_on_connect = False,
							suppress_ragged_eofs = True,
							ciphers = guc.get("ssl_ciphers")
						)
						self.connection.do_handshake()

					else:
						self.connection.send(b"N")

					LOGGER.debug("%s SSL initiation request from peer %s" % ("Granted" if (ssl_enabled) else "Denied", self))

				elif (isinstance(msg, pg_messages.StartupMessage)):

					auth_request = pg_messages.Authentication()
					auth_request.mode = pg_messages.AuthenticationMode.MD5Password
					auth_request.encode()
					self.send_message(pg_messages.PeerType.FrontEnd, auth_request)
					self.state = pg_messages.SessionState.WaitingForCredentials

				elif (isinstance(msg, pg_messages.Password)):

					auth_ok = pg_messages.Authentication()
					auth_ok.mode = pg_messages.AuthenticationMode.Ok
					auth_ok.encode()
					self.send_message(pg_messages.PeerType.FrontEnd, auth_ok)
					self.state = pg_messages.SessionState.WaitingForCommand
					pop_timeout = None


	def send_message(self, peer_type, message):
		"""
			Simply forwards the message to either side. In the case of the
			pool, this is likely to be a simple socket wakeup signal
			Args:
				peer_type:		(pg_messages.PeerType)Where to
				message:		(pg_messages.Message)What
				
			Returns the amount of bytes sent
		"""
		
		LOGGER.debug("Sending to %s: %s", peer_type.name, message)
		if (peer_type is pg_messages.PeerType.FrontEnd):
			return self.connection.send(bytes(message))
		else:
			NotImplementedException("Cannot send messages to %s yet" % (peer_type.name))


