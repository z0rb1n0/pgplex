#!/usr/bin/python3 -uB
import guc
import sys
import logging
import ipc_streams
import enum
import struct
import random


LOGGER = logging.getLogger(__name__)



"""
See https://www.postgresql.org/docs/current/static/protocol-message-formats.html
for postgres protocol documentation

"""


# this is a quick lookup table that resolves message qualifiers into the
# correct QualifiedMessage class
MESSAGE_QUALIFIERS_CLASSES = {}


class MessageEncodeError(Exception):
	pass
	
class MessageDecodeError(Exception):
	pass




# STATE FLOW MANAGEMENT
class SessionState(enum.IntEnum):
	"""
		These are possible states a postgres stream can be in,
		and by extension a pgplex *StreamSession, can be in.
	"""

	# the following only happen at startup
	WaitingForInitialMessage = 1
	WaitingForCredentials = 2
	WaitingForCommand = 3


class PeerType(enum.Enum):
	"""
		"Places" pgplex talks to. Useful to indicate directionality of a message

		Every member contains a set of the possible SessionState values a particular
		PeerType-controlled stream can possibly be in.
		
		NOTE:	the naming is somewhat counter-intuitive, as FrontEnd-type peers actually
				mean backend-type sessions and the other way around for BackEnd-type peers.
				This is because the naming indicates what's on the other side
	"""
	BackEnd = set(())
	FrontEnd = set((SessionState.WaitingForInitialMessage, SessionState.WaitingForCredentials))
	



class AuthenticationMode(enum.IntEnum):
	"""
		Possible values for the Authentication-type messages.
		They're encoded as-is
	"""
	Ok = 0
	KerberosV5 = 2
	CleartextPassword = 3
	MD5Password = 5
	SCMCredential = 6
	GSS = 7
	SSPI = 9
	GSSContinue = 9

# In order to quickly decode authentication states into enum values,
# we create a map of authentication states by number,
# And we index it by the packed value
AUTH_STATE_BYTES = {}

for auth_state in AuthenticationMode:
	AUTH_STATE_BYTES[struct.pack("!I", 0 + auth_state)] = auth_state





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
	# this is kinda pointless here but simplifies flow
	TYPE_QUALIFIER = b""
	
	# This attribute specifies the minimum amounts of bytes required to infer
	# message type and size
	SIGNATURE_SIZE = None


	# whether or not a message type should always call self.encode() when one is changed
	AUTO_ENCODE = False


	# whether or not a message type should always call self.decode() when its buffer is full
	AUTO_DECODE = False
	
	
	# This is the dictionary of states of a given subclass of message is and
	# all its subclasses in turn are accepted by backends in (it is a
	# pg_streams.BackEndState). 
	# Note that it is indexed by [PeerType] and each member is then the list
	# example override:
	# VALID_START_STATES = {
	# 		PeerType.FrontEnd] = (pg_streams.BackEndState.WaitingForInitialMessage,)
	VALID_START_STATES = {}
	

	# this dynamically initializes the special propertes of each message type,
	# (what would be members of a struct in C) without having to override each subclass __init__)
	# Each key in this dictionary is created as an object attribute, the default value being
	# the value
	PAYLOAD_MEMBERS = {}


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
				if (start_data[0] not in MESSAGE_QUALIFIERS_CLASSES):
					raise ValueError("Unknown message type qualifier: %s" % chr(start_data[0]))

				out_class = MESSAGE_QUALIFIERS_CLASSES[start_data[0]]


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


		# we initialize the payload-specific attributes.
		for pm in self.PAYLOAD_MEMBERS:
			setattr(self, pm, self.PAYLOAD_MEMBERS[pm])

		
		if (len(start_data)):
			self.append(start_data)


		#print("Init of %s completed. Data: %s" % (self.__class__.__name__, start_data))


	@property
	def missing_bytes(self):
		return ((self.length + len(self.TYPE_QUALIFIER)) - len(self.data)) if (self.length is not None) else None


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

		# time to really append however much data is missing.
		# Catch: the intended length is 1 short for qualified messages
		self.data += newdata[0:((self.length + len(self.TYPE_QUALIFIER)) - len(self.data))]


		if (self.AUTO_DECODE and self.length and (not self.missing_bytes)):
			self.decode()



	def encode(self):
		"""
			Wrapper for the actual subclass-controlled encoder. Note that no checks are in place as the hook
			can have arbitrary rules as to whether or not it is possible to proceed.

			
			encode_hook() is expected to return the PAYLOAD only: type qualifier (if any) and length are prepended by
			this function, however

		"""
		if (not hasattr(self, "encode_hook") and callable(self.encode_hook)):
			raise NotImplementedError("encode() has ben called, but %s does not implement encode_hook()" % (self.__class__.__name__))

		payload = self.encode_hook()
		self.length = 4 + len(payload)
		self.data = self.TYPE_QUALIFIER + struct.pack("!I", self.length) + payload


	def decode(self):
		"""
			Wrapper for the actual subclass-controlled decoder.
		"""

		if (not hasattr(self, "decode_hook") and callable(self.decode_hook)):
			raise NotImplementedError("decode() has ben called, but %s does not implement decode_hook()" % (self.__class__.__name__))

		if ((not self.length) or self.missing_bytes):
			raise ValueError("The message data buffer is %s" % ("empty" if (not self.length) else ("missing %d bytes" % self.missing_bytes)))

		return self.decode_hook()

	def __bytes__(self):
		return self.data

	def info_str(self):
		""" Should a specific message choose to add some more info to __str__ and __repr__ it can override this method """
		return None

	def __str__(self):
		"""
			Subclasses are allowed to add some of their own to __str__ by exposing an info_str() method
		"""
		# note that the type qualifier is NOT counted in the size uint of the data
		info_str = self.info_str()
		return ("PostgreSQL protocol message: %s[%scomplete, stated size: %s, current_size: %d]%s" % (
			self.__class__.__name__,
			("" if ((len(self.data) - len(self.TYPE_QUALIFIER)) == (self.length if (self.length is not None) else -1)) else "in"),
			(str(self.length) if (self.length is not None) else "<unknown>"),
			len(self.data),
			("( " + (info_str if (info_str) else "undecoded") + " )")
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

	AUTO_DECODE = True

	# this can safely be inherited by all startup message types
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForInitialMessage,)
	}


class StartupMessage(InitialMessage):
	"""
		This is what starts it all. The only piece of information is the
		connection string, which needs to be parsed manually anyway
	"""
	PAYLOAD_MEMBERS = {
		"protocol_major": None,
		"protocol_minor": None,
		"options": {}
	}


	def append(self, newdata):
		""" The startup message is always decoded when possible """
		super().append(newdata)

		# this message is always decoded regardless
		self.decode()


	def decode_hook(self):
		""" Just extracts version information """
		(self.protocol_major, self.protocol_minor) = struct.unpack("!HH", (self.data[4:6] + self.data[6:8]))

		# we break the connection string into pieces
		opt_n = None
		for cnn_opt_w in self.data[8:-1].split(b"\x00"):
			if (opt_n is None):
				opt_n = cnn_opt_w.decode()
			else:
				self.options[opt_n] = cnn_opt_w.decode()
				opt_n = None

		return True

	def info_str(self):
		if ((self.protocol_major and self.protocol_minor) is not None):
			return "protocol version: %d.%d, options: %s" % (self.protocol_major, self.protocol_minor, str(self.options))
		else:
			return "protocol information unknown"


class CancelRequest(InitialMessage):
	ALLOWED_RECIPIENTS = PeerType.BackEnd
	AUTO_DECODE = True
	RESERVED_PROTOCOL_VERSION = (1234, 5678)


class SSLRequest(InitialMessage):
	ALLOWED_RECIPIENTS = PeerType.BackEnd
	AUTO_DECODE = True
	RESERVED_PROTOCOL_VERSION = (1234, 5679)

	def decode_hook(self):
		""" No data here """
		return b""



class QualifiedMessage(Message):
	"""
		Base class for all the modern postgres message that have types marker bytes
		at the beginning (the vast majority).
	"""
	SIGNATURE_SIZE = 5 # see base class

	# At module initialization, this class member is meant to be enumerated for all
	# the subclasses of this class and put into a dictionary for quick type resolution

	 # this is not valid
	TYPE_QUALIFIER = b"\x00"


class CommandComplete(QualifiedMessage):
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"C"


class Authentication(QualifiedMessage):
	"""
		Any of AuthenticationKerberosV5, AuthenticationCleartextPassword and so on
		encode() is called
	"""
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"R"
	PAYLOAD_MEMBERS = {
		"mode": None,
		"salt": None,
		"gss_sspi_data": None
	}


	def encode_hook(self):
		
		if (self.mode is None):
			raise(MessageEncodeError("mode is not set"))
		
		if ((self.mode == AuthenticationMode.MD5Password) and (self.salt is None)):
			# we just generate one
			self.salt = random.randint(-2147483648, 2147483647)

		if ((self.mode == AuthenticationMode.GSSContinue) and (self.gss_sspi_data is None)):
			raise(MessageEncodeError("GSSAPI/SPI authentication data is required"))
		
		out_str = struct.pack("!I", self.mode)
		
		if (self.mode == AuthenticationMode.MD5Password):
			out_str += struct.pack("!i", self.salt)
		elif (self.mode == AuthenticationMode.GSSContinue):
			out_str += self.gss_sspi_data

		return out_str


	# todo: decode hook for the backend side

	def info_str(self):
	
		if (self.mode is not None):
			return "mode: %s" % (self.mode.name,)
		else:
			return "Unknown authentication mode"



class Password(QualifiedMessage):
	"""
		Any of AuthenticationKerberosV5, AuthenticationCleartextPassword and so on
		encode() is called
	"""
	ALLOWED_RECIPIENTS = PeerType.BackEnd
	TYPE_QUALIFIER = b"p"
	PAYLOAD_MEMBERS = {
		"password": None
	}
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForCredentials,)
	}

	AUTO_DECODE = True
	def decode_hook(self):
		self.password = self.data[self.SIGNATURE_SIZE:].decode()
		return True

	def encode_hook(self):
		if (not isinstance(self.password, str)):
			raise MessageEncodeError("Password property is not set/invalid")
		return self.password.encode()


	def info_str(self):
		return "<security info hidden>"



# we go through the classes and build a message type resolver now.
for (name, msg_class) in dict(sys.modules[__name__].__dict__.items()).items():
	if ((isinstance(msg_class, type)) and issubclass(msg_class, QualifiedMessage) and (msg_class is not QualifiedMessage)):
		if (msg_class.TYPE_QUALIFIER == QualifiedMessage.TYPE_QUALIFIER):
			raise AttributeError("Class %s is a subclass of QualifiedMessage but does override its TYPE_QUALIFIER" % (msg_class.__name__))
		if (len(msg_class.TYPE_QUALIFIER) != 1):
			raise ValueError("Invalid TYPE_QUALIFIER lenght for class %s" % (msg_class.__name__))

		# note that we store it as an integer
		MESSAGE_QUALIFIERS_CLASSES[msg_class.TYPE_QUALIFIER[0]] = msg_class

	# also, we pack some attribute to avoid having to do it at "run-time"
	if ((isinstance(msg_class, type)) and issubclass(msg_class, InitialMessage)):
		if (msg_class.RESERVED_PROTOCOL_VERSION is not None):
			msg_class.RESERVED_PROTOCOL_VERSION_PACKED = b"".join(
				map(lambda v : v.to_bytes(length = 2, byteorder = "big"), msg_class.RESERVED_PROTOCOL_VERSION)
			)


