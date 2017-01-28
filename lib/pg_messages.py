#!/usr/bin/python3 -uB
import guc
import sys
import logging
import ipc_streams
import enum
import struct


LOGGER = logging.getLogger(__name__)


# this is a quick way we use to resolve a message 
MESSAGE_TYPE_CLASSES = {}


class PeerTypes(enum.IntEnum):
	"""
		"Places" pgplex talks to. Useful to indicate directionality of a message
	"""
	Neither = 0 # Placeholder for Initialization
	Backend = 1
	Frontend = 2



class Message(object):
	"""
		Base class all postgres protocol message classes are constructed from.

		Messages can be constructed both from a data buffer (usually from the network),
		or by setting message type-specific properties (generally defined in subclasses
		but some apply to all message types).


		CONSTRUCTION THROUGH BUFFER:

		The message has an internal buffer, which can accumulate data in the following
		ways, up to the intended message length.
		
		- start_data is passed to the constructor (useful for testing)
		- a string is passed to the append() (from external sources)

		Since the message length can only be guessed after a few bytes have been read,
		it is generally accepted that the initial buffer is larger than the whole message and
		contains data from followin messages. The extra data will be ignored.

		The decode() method will attempt to parse the message and populate the relevant class members,
		but this can only happen if all of  the following conditions are met:

		- The buffer length matches the message's intended length
		- The subclass (or the instance...) implements a decode_hook() method

		CONSTRUCTION THROUGH MEMBERS:

		Members can be populated manually. And encode() can be called in order to
		fill the buffer, so long as encode_hook() is implemented in the subclass.
	

	"""

	# this technically does not belong here, however it makes everything easier
	type_qualifier = ""
	
	# by default messages go nowhere
	allowed_targets = PeerTypes.Neither

	def __init__(self,
		start_data = b""
	):
		"""
			The message itself can be initialized empty, however that wil
		"""

		# total INTENDED lenght of the message, excluding the type qualifier
		self.length = None

		# the data buffer
		self.buffer = b""

		if (len(start_data)):
			self.append(start_data)

		

	@property
	def missing_bytes(self):
		return (self.length - self.data) if (self.length is not None) else None


	def append(self, newdata):
		"""
			Adds data to the buffer, unless the buffer is already full
			
			Args:
				newdata:		(str)The appendee
		"""
		# we need to determine message length, and how we do it changes based on
		# whether or not the message is qualified
		if (self.length is None):
			# Length still unknown
			min_len = len(self.type_qualifier) + 4
			if ((len(self.buffer) + len(newdata)) >= min_len):

				# between our previous buffer and the new data we've got enough bytes
				# It's a little tricky as we're potentially operating across 2 strings
				# (the previously buffered data)
				self.length = struct.unpack("!I", (self.buffer + newdata[0:(min_len - len(self_buffer))])[len(self.type_qualifier):])[0]

		# time to really append however much data is missing
		self.buffer += newdata[0:(self.length - len(self.buffer))]



	def encode(self):
		if (len(self.buffer) < self.length):
			raise ValueError("The message buffer is not complete")
			return None
		""" Wrapper for the actual subclass-controlled encoder """
		if (not hasattr(self, "encode_hook") and callable(self.encode_hook)):
			raise NotImplementedError("encode() has ben called, but %s does not implement encode_hook()" % (self.__class__.__name__))
			return None
		self.encode_hook()

		

	def decode(self):
		""" Wrapper for the actual subclass-controlled encoder """
		if (not hasattr(self, "encode_hook") and callable(self.encode_hook)):
			raise NotImplementedError("decode() has ben called, but %s does not implement encode_hook()" % (self.__class__.__name__))
		self.encode_hook()


class UnqualifiedMessage(Message):
	"""
		For historical reasons, startup messages in postgres have no qualifier
		byte. They'll be subclassed directly from this class
		StartupMessage, SSLRequest, CancelRequest
		
		
		The way this construct
		
	"""


class SSLRequest(UnqualifiedMessage):
	pass



class QualifiedMessage(Message):
	"""
		Base class for all the modern postgres message that have types marker bytes
		at the beginning (the vast majority).
		
		https://www.postgresql.org/docs/current/static/protocol-message-formats.html
	"""
	# At module initialization, this class member is meant to be enumerated for all
	# the subclasses of this class and put into a dictionary for quick type resolution
	type_qualifier = b"\x00" # this is not valid




class CommandCompleteMessage(Message):
	allowed_targets = PeerTypes.Frontend
	type_qualifier = b"C"



# we build the message class resolver now
for (name, msg_class) in dict(sys.modules[__name__].__dict__.items()).items():
	if ((isinstance(msg_class, type)) and issubclass(msg_class, QualifiedMessage) and (msg_class is not QualifiedMessage)):
	#if (issubclass(thing, Message)):
		if (msg_class.type_qualifier == QualifiedMessage.type_qualifier):
			raise AttributeError("Class %s is a subclass of QualifiedMessage but does override its type_qualifier" % (msg_class.__name__))
		if (len(msg_class.type_qualifier) != 1):
			raise ValueError("Invalid type_qualifier lenght for class %s" % (msg_class.__name__))

		MESSAGE_TYPE_CLASSES[msg_class.type_qualifier] = msg_class


