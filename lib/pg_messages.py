#!/usr/bin/python3 -uB
import sys
import logging
import enum
import struct
import random

import ipc_streams
import guc
import pg_data


LOGGER = logging.getLogger(__name__)






"""
See https://www.postgresql.org/docs/current/static/protocol-message-formats.html
for postgres protocol documentation



All packed integer are assumed to be signed. Couldn't find any mention of
signedness in the docs (they only mention endianness) but they do make use of negative
integers





A lot of refactoring is called for, primarily for performance reasons:


# this is how we can address this issue efficiently:
import ctypes as c
import array as a

class MessageQualifier(c.c_char):
	pass

class MessageSize(c.c_uint32):
	pass

class QualifiedMessage(c.BigEndianStructure):
	_pack_ = 0
	_fields_ = [
		("qualifier", MessageQualifier),
		("size", MessageSize),
		("payload", c.c_char * 256)
	]


buf = bytearray(c.sizeof(QualifiedMessage))

msg = QualifiedMessage.from_buffer(buf, 0)
print(buf)
msg.qualifier = b"c"
msg.size = c.sizeof(msg) - c.sizeof(MessageQualifier)
msg.payload = b"hello"
print(buf)





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

	# Generally Backend
	WaitingForInitialMessage = 1
	WaitingForCredentials = 2
	SettingUpSession = 3
	WaitingForQuery = 4
	Active = 5


	# Generally Frontend
	WaitingForSSLClearance = 1025
	WaitingForAuthenticationMode = 1026
	WaitingForSessionSetup = 1027
	Idling = 1028
	WaitingForResultState = 1029




class PeerType(enum.Enum):
	"""
		"Places" pgplex talks to. Useful to indicate directionality of a message

		Every member contains a set of the possible SessionState values a particular
		stream peer type
		
		

	"""
	BackEnd = set((
		SessionState.WaitingForInitialMessage, SessionState.WaitingForCredentials,
		SessionState.SettingUpSession, SessionState.Active
	))
	FrontEnd = set((
		SessionState.WaitingForSSLClearance, SessionState.WaitingForAuthenticationMode,
		SessionState.WaitingForSessionSetup
	))
	

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




class TransactionState(enum.IntEnum):
	"""
		Possible values for the transaction state of a Session.

		They encode the characters for the state
	"""
	Idle = ord("I")
	IdleInTransaction = ord("T")
	TransactionFailed = ord("E")






# In order to quickly decode authentication states into enum values,
# we create a map of authentication states by number,
# And we index it by the packed value
AUTH_STATE_BYTES = {}

for auth_state in AuthenticationMode:
	AUTH_STATE_BYTES[struct.pack("!i", 0 + auth_state)] = auth_state



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
		
		Message types that are purely comprised by their signature can omit the encoding/
		decoding hook implementations
		

	"""
	
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
	# 		PeerType.BackendEnd] = (pg_streams.BackEndState.WaitingForInitialMessage,)
	VALID_START_STATES = {}


	# this dynamically initializes the special propertes of each message type,
	# (what would be members of a struct in C) without having to override each subclass __init__)
	# Each key in this dictionary is created as an object attribute, the default value being
	# the value in the definition itself.
	PAYLOAD_MEMBERS = {}

	@classmethod
	def from_buffer(cls, start_data):
		"""
			Accepts a bytearray structure and instantiates a Message subclass.

			Depending what we've got in our hands, we determine which REAL class needs
			to be constructed.

			We only try to get guess the type when instatiation happens from the
			generic classes for the 2 message types (InitialMessage and QualifiedMessage)

			Message cannot be instaniated directly (it's kinda abstract)
			

			Ideally we should start using ctypes structures for this
			
			
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

			if (not (len(start_data) >= cls.SIGNATURE_SIZE)):
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

		out_obj = out_class()
		out_obj.append(start_data)

		return out_obj


	def __init__(self):
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


		#print("Init of %s completed. Data: %s" % (self.__class__.__name__, start_data))


	@property
	def missing_bytes(self):
		return ((self.length + self._qualifier_lenght) - len(self.data)) if (self.length is not None) else None


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
				(self.length,) = struct.unpack("!i", (self.data + newdata[0:(self.SIGNATURE_SIZE - len(self.data))])[(self.SIGNATURE_SIZE - 4):])

		# time to really append however much data is missing.
		# Catch: the intended length is 1 short for qualified messages
		self.data += newdata[0:((self.length + self._qualifier_lenght) - len(self.data))]


		if (self.AUTO_DECODE and self.length and (not self.missing_bytes)):
			self.decode()



	def encode(self):
		"""
			Wrapper for the actual subclass-controlled encoder. Note that no checks are in place as the hook
			can have arbitrary rules as to whether or not it is possible to proceed.

			encode_hook() is expected to return the PAYLOAD only: type qualifier (if any) and length are prepended by
			this function, however

		"""

		# encode hook is not needed if the message is just its signature
		if (self.PAYLOAD_MEMBERS):
			if (not (hasattr(self, "encode_hook") and callable(self.encode_hook))):
				raise NotImplementedError("encode() has ben called, but %s does not implement encode_hook()" % (self.__class__.__name__))
			payload = self.encode_hook()
		else:
			payload = b""

		self.length = 4 + len(payload)
		self.data = (self.TYPE_QUALIFIER if self._qualifier_lenght else b"") + struct.pack("!i", self.length) + payload


	def decode(self):
		"""
			Wrapper for the actual subclass-controlled decoder.
		"""

		if ((not self.length) or self.missing_bytes):
			raise ValueError("The message data buffer is %s" % ("empty" if (not self.length) else ("missing %d bytes" % self.missing_bytes)))

		# decode hook is not needed if the message is just its signature
		if (self.PAYLOAD_MEMBERS):
			if (not hasattr(self, "decode_hook") and callable(self.decode_hook)):
				raise NotImplementedError("decode() has ben called, but %s does not implement decode_hook()" % (self.__class__.__name__))

			return self.decode_hook()
		else:
			return True

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
			("" if ((len(self.data) - self._qualifier_lenght) == (self.length if (self.length is not None) else -1)) else "in"),
			(str(self.length) if (self.length is not None) else "<unknown>"),
			len(self.data),
			("( " + (info_str if (info_str) else "<Payload not described>") + " )")
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
		PeerType.BackEnd: (SessionState.WaitingForInitialMessage,)
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

	AUTO_DECODE = True


	def decode_hook(self):
		""" Just extracts version information """
		(self.protocol_major, self.protocol_minor) = struct.unpack("!hh", (self.data[4:6] + self.data[6:8]))

		# we break the connection string into pieces
		opt_n = None
		for cnn_opt_w in self.data[8:-1].split(b"\x00"):
			if (opt_n is None):
				opt_n = cnn_opt_w
			else:
				self.options[opt_n] = cnn_opt_w
				opt_n = None

		return True

	def info_str(self):
		if ((self.protocol_major and self.protocol_minor) is not None):
			return "protocol version: %d.%d, options: %s" % (self.protocol_major, self.protocol_minor, self.options)
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
		
		
		Any subclass, in order to be usable, needs to define its own
		ALLOWED_RECIPIENTS and TYPE_QUALIFIER (the single-byte marker at the beginning of the message)

		EG:
		ALLOWED_RECIPIENTS = PeerType.BackEnd
		TYPE_QUALIFIER = b"X"
	"""
	SIGNATURE_SIZE = 5 # see base class

	

class SignatureOnlyMessage(QualifiedMessage):
	"""
		Base class for all qualified message types that are defined by
		signature only (their only difference is the qualifier).
		
		They're all auto-decoding
	"""
	AUTO_DECODE = True

class Terminate(SignatureOnlyMessage):
	ALLOWED_RECIPIENTS = PeerType.BackEnd
	TYPE_QUALIFIER = b"X"
	VALID_START_STATES = {
		PeerType.BackEnd: (SessionState.WaitingForQuery,)
	}

class EmptyQueryResponse(SignatureOnlyMessage):
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"I"
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForResultState,)
	}

class NoData(SignatureOnlyMessage):
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"n"
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForResultState,)
	}


class ResponseToUser(QualifiedMessage):
	"""
		Template class for all "response" type message types (eg: NoticeResponse, ErrorResponse)
		
		Since they're structurally identical, the hooks are the same.

		(Decoding is tricky and requires a bit of parsing)

	"""
	PAYLOAD_MEMBERS = {
		# this is an array of tuples. It's literally the messages' text.
		# Note that they're internally encoded in UTF8 regardless of target encoding
		"messages": []
	}
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForResultState,)
	}
	def decode_hook(self):
		self.messages = [(ml[:1], ml[1:]) for ml in self.data[self.SIGNATURE_SIZE:-1].split(b"\x00\x00")]

		return True

	def encode_hook(self):
		if (not len(self.messages)):
			raise MessageEncodeError("There are no message strings")
		return b"\x00".join(((mh + mb) for (mh, mb) in self.messages)) + b"\x00\x00"
	
	def info_str(self):
		return "%s" % (self.messages,)

class NoticeResponse(ResponseToUser):
	"""
	"""
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"N"

class ErrorResponse(ResponseToUser):
	"""
	"""
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"E"




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
	AUTO_DECODE = True

	def encode_hook(self):
		
		if (self.mode is None):
			raise(MessageEncodeError("mode is not set"))

		if ((self.mode == AuthenticationMode.MD5Password) and (self.salt is None)):
			# we just generate one
			self.salt = random.randint(-2147483648, 2147483647)

		if ((self.mode == AuthenticationMode.GSSContinue) and (self.gss_sspi_data is None)):
			raise(MessageEncodeError("GSSAPI/SPI authentication data is required"))
		
		out_str = struct.pack("!i", self.mode)
		
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
		PeerType.BackEnd: (SessionState.WaitingForCredentials,)
	}

	AUTO_DECODE = True
	def decode_hook(self):
		self.password = self.data[self.SIGNATURE_SIZE:-1]
		return True

	def encode_hook(self):
		if (not isinstance(self.password, bytes)):
			raise MessageEncodeError("Password property is not set/invalid")
		return self.password


	def info_str(self):
		return "<security info hidden>"


class BackendKeyData(QualifiedMessage):
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"K"
	PAYLOAD_MEMBERS = {
		"backend_pid": None,
		"secret_key": None
	}
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForSessionSetup,)
	}

	AUTO_DECODE = True

	def decode_hook(self):
		(self.backend_pid) = struct.unpack("!i", self.data[self.SIGNATURE_SIZE:4])
		self.secret_key = self.data[self.SIGNATURE_SIZE + 4:4]
		return True

	def encode_hook(self):
		return struct.pack("!i", self.backend_pid) + self.secret_key

	def info_str(self):
		return "process ID: %d, secret key: `%s`" % (self.backend_pid, self.secret_key)



class ReadyForQuery(QualifiedMessage):
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"Z"
	PAYLOAD_MEMBERS = {
		"transaction_state": None
	}
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForSessionSetup,)
	}
	AUTO_DECODE = True
	# unfortunately in the case of ReadyForQuery we need to create a member that allows to translate
	# a number into an enum member
	TS_MAP = {}

	def decode_hook(self):
		state_code = ord(self.data[self.SIGNATURE_SIZE:1])

		try:
			self.transaction_state = self.TS_MAP[state_code]
		except KeyError:
			raise MessageDecodeError("Invalid transaction state byte: %d" % state_code)

		return True

	def encode_hook(self):
		return bytes([self.transaction_state.value])

	def info_str(self):
		return self.transaction_state.name

# here we backfill the translation map
for m in TransactionState:
	ReadyForQuery.TS_MAP[m.value] = m
	


class ParameterStatus(QualifiedMessage):
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"S"
	PAYLOAD_MEMBERS = {
		"parameter": None,
		"value": None
	}
	VALID_START_STATES = {
		PeerType.FrontEnd: (SessionState.WaitingForSessionSetup, SessionState.Idling)
	}
	AUTO_DECODE = True

	def decode_hook(self):
		(self.parameter, self.value) = self.data[self.SIGNATURE_SIZE:-1].split(b"\x00")
		return True


	def encode_hook(self):
		return b"\x00".join((self.parameter, self.value)) + b"\x00"
		return True

	def info_str(self):
		return "%s = %s" % (self.parameter, self.value)
	


class Query(QualifiedMessage):
	"""
		Simple query protocol. Will always be my favorite
	"""
	ALLOWED_RECIPIENTS = PeerType.BackEnd
	TYPE_QUALIFIER = b"Q"
	PAYLOAD_MEMBERS = {
		"query": None
	}
	VALID_START_STATES = {
		PeerType.BackEnd: (SessionState.WaitingForQuery,)
	}

	AUTO_DECODE = False
	def decode_hook(self):
		self.query = self.data[self.SIGNATURE_SIZE:-1]
		return True

	def encode_hook(self):
		if (not isinstance(self.query, bytes)):
			raise MessageEncodeError("Query is not set")
		return self.password

	def info_str(self):
		return self.query.replace(b"\x10", b"\x10\x10").decode()[0:32]



class RowDescription(QualifiedMessage):
	"""
		Relatively complex message type, mostly because one of the fields is
		variable-lenght (the field name).
		
		Could probably be implemented with secondary inheritance from pg_data.RowDefinition,
		but we'd still have to maintain "fields" manually, so we proxy some of the
		methods under different names
		
		The constructor allows for an iterable of fields to be passed. EG:

			rd = RowDescription([
				pg_data.FieldDefinition(name = "foo"),
				pg_data.FieldDefinition(name = "bar", type_len = 1234)
			])

	"""
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"T"
	# the "fields" is just an instance of the row description, empty by default
	PAYLOAD_MEMBERS = {
		"fields": pg_data.RowDefinition()
	}
	AUTO_DECODE = False

	def __init__(self, initial_fields = ()):
		"""
			The constructor accepts a list.
			We pre-hydrate the "fields" member with that list of FieldDefinitions
		"""
		super().__init__()
		# ugly method proxying
		[setattr(self, proxied, getattr(self.fields, proxied)) for proxied in (
			"append", "insert", "extend", "__iadd__", "__imul__", "pop"
		)]
		self.extend(initial_fields)





################################################################
################################################################
### MODULE INITIALIZATION
################################################################
################################################################


# we go through the classes and build a message type resolution dictionary now
for (name, msg_class) in dict(sys.modules[__name__].__dict__.items()).items():
	# if the message has a non-empty type qualifier, we ensure that it is valid
	if (isinstance(msg_class, type)):
		
		# in order to avoid using "hasattr" too much at run-time, we store a
		# convenience "_qualifier_lenght"  class member for all message classes
		if (issubclass(msg_class, Message)):
			if (issubclass(msg_class, QualifiedMessage) and (msg_class is not QualifiedMessage)):
				msg_class._qualifier_lenght = 1
				if (hasattr(msg_class, "ALLOWED_RECIPIENTS")):
					if (not hasattr(msg_class, "TYPE_QUALIFIER")):
						raise AttributeError("Class %s is a subclass of QualifiedMessage which is active by defining ALLOWED_RECIPIENTS but does not define its TYPE_QUALIFIER member (bust be a single byte)" % (msg_class.__name__))

					if (len(msg_class.TYPE_QUALIFIER) != 1):
						raise ValueError("Invalid TYPE_QUALIFIER lenght for class %s" % (msg_class.__name__))

					# note that we store it as an integer
					MESSAGE_QUALIFIERS_CLASSES[msg_class.TYPE_QUALIFIER[0]] = msg_class

			else:
				msg_class._qualifier_lenght = 0


	# also, we pack some attribute to avoid having to do it at "run-time"
	if ((isinstance(msg_class, type)) and issubclass(msg_class, InitialMessage)):
		if (msg_class.RESERVED_PROTOCOL_VERSION is not None):
			msg_class.RESERVED_PROTOCOL_VERSION_PACKED = b"".join(
				map(lambda v : v.to_bytes(length = 2, byteorder = "big"), msg_class.RESERVED_PROTOCOL_VERSION)
			)


