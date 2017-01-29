#!/usr/bin/python3 -uB
import guc
import sys
import logging
import ipc_streams
import enum
import struct


LOGGER = logging.getLogger(__name__)






# this is a quick way we use to resolve a message 
QUALIFIED_MESSAGE_TYPE_SIGNATURE_MAPS = {}


# recursive dictionary whigh specifies what
SUPPORTED_PROTOCOL_VERSIONS = {
	3: (0,)
}



class PeerTypes(enum.IntEnum):
	"""
		"Places" pgplex talks to. Useful to indicate directionality of a message
	"""
	Neither = 0 # Placeholder for Initialization
	BackEnd = 1
	FrontEnd = 2



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
	# This attribute specifies the minimum amounts of bytes required to infer
	# message type and size
	SIGNATURE_SIZE = None

	# base messages go nowhere
	ALLOWED_RECIPIENTS = PeerTypes.Neither
	
	

	def __new__(cls, *args, **kwargs):
		"""
			Depending what we've got in our hands, we determine which REAL class needs
			to be constructed.

			We only try to get guess the type when instatiation happens from the
			generic classes for the 2 message types (InitialMessage and QualifiedMessage)
			
			Message cannot be instaniated directly (it's kinda abstract)
			
		"""

		# some duck typing to find out when to fail or what to do
		if (cls.SIGNATURE_SIZE is None):
			raise TypeError("%s cannot be instantiated directly. Please use a subclass" % cls.__name__)


		# Intermediate, generic subclasses are also factories and their constructor initiates the correct
		# subclass
		# This check is done on the class name to avoid having dealing with circular dependencies


		cls_name = cls.__name__
		out_class = cls

		if (cls_name in ("InitialMessage", "QualifiedMessage")):

			#LOGGER.debug("Magic message type `%s` was instantiated" % cls_name)

			if (len(args) and isinstance(args[0], bytes) and (len(args[0]) >= cls.SIGNATURE_SIZE)):
				start_data = args[0]
			elif (("start_data" in kwargs) and isinstance(args["kwargs"], bytes) and (len(args["kwargs"]) >= cls.SIGNATURE_SIZE)):
				start_data = kwargs["start_data"]
			else:
				raise ValueError("Magic instantiation of %s requires a `bytes` instace at least %d bytes long as the start data" % (
					cls.__name__,
					cls.SIGNATURE_SIZE
				))
				return None

			out_class = None
			
			
			if (cls_name == "InitialMessage"):
				# there's a very small number of initial messages. No need for a lookup table yet
				for im_class in cls.__subclasses__():
					if (im_class.RESERVED_PROTOCOL_VERSION_PACKED == start_data[4:(4 + cls.SIGNATURE_SIZE)]):
						out_class = im_class
						break

				# still no match? that means it's a StartupMessage
				if (out_class is None):
					out_class = StartupMessage
			else:
				# we use the  first byte for the magical lookup
				try:
					out_class = QUALIFIED_MESSAGE_TYPE_SIGNATURE_MAPS[start_data[0]]
				except KeyError as ub_err:
					raise ValueError("Unknown message type qualifier: %s" % start_data[0])


		out_obj = object.__new__(out_class)
		return out_obj


	def __init__(self,
		start_data = b""
	):
		"""
			The message itself can be initialized empty
		"""


		# total INTENDED lenght of the message, excluding the type qualifier
		self.length = None

		# the data itself
		self.data = b""
		
		if (len(start_data)):
			self.append(start_data)


		#print("Init of %s completed. Data: %s" % (self.__class__.__name__, start_data))


	@property
	def missing_bytes(self):
		return (self.length - len(self.data)) if (self.length is not None) else None


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
			if ((len(self.data) + len(newdata)) >= self.SIGNATURE_SIZE):

				# between our previous buffer and the new data we've got enough bytes
				# It's a little tricky as we're potentially operating across 2 strings
				# (the previously buffered data)
				(self.length,) = struct.unpack("!I", (self.data + newdata[0:(self.SIGNATURE_SIZE - len(self.data))])[(self.SIGNATURE_SIZE - 4):])
				# old calls:
				#self.length = int.from_bytes(
				#	bytes = (self.data + newdata[0:(self.SIGNATURE_SIZE - len(self.data))])[(self.SIGNATURE_SIZE - 4):],
				#	byteorder = "big",
				#	signed = False # this is a guess. Can't find docs
				#)
				#self.length = struct.unpack("!I", (self.data + newdata[0:(min_len - len(self_buffer))])[len(self.TYPE_QUALIFIER):])[0]

		# time to really append however much data is missing
		self.data += newdata[0:(self.length - len(self.data))]



	def encode(self):
		"""
			Wrapper for the actual subclass-controlled encoder. Note that no check are in place as the hook
			can have arbitrary rules as to whether or not it is possible to proceed
		"""
		if (not hasattr(self, "encode_hook") and callable(self.encode_hook)):
			raise NotImplementedError("encode() has ben called, but %s does not implement encode_hook()" % (self.__class__.__name__))
			return None
		return self.encode_hook()

		

	def decode(self):
		"""
			Wrapper for the actual subclass-controlled decoder. Note that no check are in place as the hook
			can have arbitrary rules as to whether or not it is possible to proceed
		"""
		if (not hasattr(self, "decode_hook") and callable(self.decode_hook)):
			raise NotImplementedError("decode() has ben called, but %s does not implement decode_hook()" % (self.__class__.__name__))
		return self.decode_hook()

	def __bytes__(self):
		return self.data

	def info_str(self):
		""" Should a specific message choose to add some more info to __str__ and __repr__ it can override this method """
		return None

	def __str__(self):
		"""
			Subclasses are allowed to add some of their own to repr by exposing a property
		"""
		info_str = self.info_str()
		return ("PostgreSQL protocol message: %s[%scomplete, stated size: %s, current_size: %d]%s" % (
			self.__class__.__name__,
			("" if (len(self.data) == (self.length if (self.length is not None) else -1)) else "in"),
			(str(self.length) if (self.length is not None) else "<unknown>"),
			len(self.data),
			("( " + info_str + " )" if (info_str) else "")
		))
		
	def __repr__(self):
		return "< %s >" % self.__str__()


class InitialMessage(Message):
	"""
		For historical reasons, startup messages in postgres have no qualifier
		byte. They'll be subclassed directly from this class
		StartupMessage, SSLRequest, CancelRequest
		The way this construct
	"""
	SIGNATURE_SIZE = 4 # see base class

	# some of the unqualified message types usurp the protocol version declaration
	# state their message type instead (eg: CancelRequest).
	# message types that do
	RESERVED_PROTOCOL_VERSION = None
	# this is just for speed
	RESERVED_PROTOCOL_VERSION_PACKED = None




class StartupMessage(InitialMessage):
	ALLOWED_RECIPIENTS = PeerTypes.BackEnd

	def __init__(self, start_data):

		self.protocol_major = None
		self.protocol_minor = None
		super().__init__(start_data)


	def append(self, newdata):
		""" The startup message is always decoded when possible """
		super().append(newdata)

		# this message is always decoded regardless
		self.decode()


	def decode_hook(self):
		""" Just extracts version information """
		if (self.length and (not self.missing_bytes)):
			(self.protocol_major, self.protocol_minor) = struct.unpack("!HH", (self.data[4:6] + self.data[6:8]))
			return True

	def info_str(self):
		if ((self.protocol_major and self.protocol_minor) is not None):
			return "protocol version: %d.%d" % (self.protocol_major, self.protocol_minor)
		else:
			return "protocol information unknown"



		

class CancelRequest(InitialMessage):
	ALLOWED_RECIPIENTS = PeerTypes.BackEnd
	RESERVED_PROTOCOL_VERSION = (1234, 5678)


class SSLRequest(InitialMessage):
	ALLOWED_RECIPIENTS = PeerTypes.BackEnd
	RESERVED_PROTOCOL_VERSION = (1234, 5679)



class QualifiedMessage(Message):
	"""
		Base class for all the modern postgres message that have types marker bytes
		at the beginning (the vast majority).
		
		https://www.postgresql.org/docs/current/static/protocol-message-formats.html
	"""
	SIGNATURE_SIZE = 5 # see base class

	# At module initialization, this class member is meant to be enumerated for all
	# the subclasses of this class and put into a dictionary for quick type resolution

	 # this is not valid
	TYPE_QUALIFIER = b"\x00"


class CommandCompleteMessage(Message):
	ALLOWED_RECIPIENTS = PeerTypes.FrontEnd
	TYPE_QUALIFIER = b"C"



# we go through the classes and build a message type resolver now.
for (name, msg_class) in dict(sys.modules[__name__].__dict__.items()).items():
	if ((isinstance(msg_class, type)) and issubclass(msg_class, QualifiedMessage) and (msg_class is not QualifiedMessage)):
		if (msg_class.TYPE_QUALIFIER == QualifiedMessage.TYPE_QUALIFIER):
			raise AttributeError("Class %s is a subclass of QualifiedMessage but does override its TYPE_QUALIFIER" % (msg_class.__name__))
		if (len(msg_class.TYPE_QUALIFIER) != 1):
			raise ValueError("Invalid TYPE_QUALIFIER lenght for class %s" % (msg_class.__name__))

		QUALIFIED_MESSAGE_TYPE_SIGNATURE_MAPS[msg_class.TYPE_QUALIFIER] = msg_class

	# also, we pack some attribute to avoid having to do it at "run-time"
	if ((isinstance(msg_class, type)) and issubclass(msg_class, InitialMessage)):
		if (msg_class.RESERVED_PROTOCOL_VERSION is not None):
			msg_class.RESERVED_PROTOCOL_VERSION_PACKED = b"".join(
				map(lambda v : v.to_bytes(length = 2, byteorder = "big"), msg_class.RESERVED_PROTOCOL_VERSION)
			)

