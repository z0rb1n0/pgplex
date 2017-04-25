#!/usr/bin/python3 -uB
import sys
import logging
import enum
import struct
import random
import copy

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



At the moment, encoding is always assumed to be UTF8, but that will be easy to change
later as str.encode() accepts an argument




A lot of refactoring may be called for, primarily for performance reasons:


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


	# some packers are better compiled
	_LENGTH_PACKER = struct.Struct("!i")

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

	from_bytes = from_buffer


	def reset_fields(self):
		"""
			Resets the "fields" attribute to the value if PAYLOAD_MEMBERS
		"""
		# we initialize the fields
		self.fields = copy.deepcopy(self.PAYLOAD_MEMBERS)
		# we initialize the payload-specific attributes.
		for pm in self.PAYLOAD_MEMBERS:
			setattr(self, pm, self.fields[pm])

	def __init__(self):
		"""
			The message itself can be initialized empty
		"""

		# total INTENDED lenght of the message, excluding the type qualifier
		# but including the length indicator itself
		self.length = None

		# the data itself, including everything, even the eventual type qualifier which
		# is NOT counted in "length"
		self.data = b""

		self.reset_fields()


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
				(self.length,) = self._LENGTH_PACKER.unpack((self.data + newdata[0:(self.SIGNATURE_SIZE - len(self.data))])[(self.SIGNATURE_SIZE - self._LENGTH_PACKER.size):])

		# time to really append however much data is missing.
		# Catch: the intended length is 1 short for qualified messages
		# This is possibly the most expensive operation in the entire class and
		# it'd make sense to look in some memoryview() trickery to make it better
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
			if (payload is None):
				raise ValueError("%s.encode_hook() returned None" % self.__class__.__name__)
		else:
			payload = b""

		self.length = self._LENGTH_PACKER.size + len(payload)
		self.data = (self.TYPE_QUALIFIER if self._qualifier_lenght else b"") + self._LENGTH_PACKER.pack(self.length) + payload


	def decode(self):
		"""
			Wrapper for the actual subclass-controlled decoder.
		"""

		# we reset it all
		self.reset_fields()
		

		if ((not self.length) or self.missing_bytes):
			raise ValueError("The message data buffer is %s" % ("empty" if (not self.length) else ("missing %d bytes" % self.missing_bytes)))

		# decode hook is not needed if the message is just its signature
		if (self.PAYLOAD_MEMBERS):
			if (not (hasattr(self, "decode_hook") and callable(self.decode_hook))):
				raise NotImplementedError("decode() has ben called, but %s does not implement a callable decode_hook()" % (self.__class__.__name__))

			return self.decode_hook()
		else:
			return True

	def __bytes__(self):
		return self.data

	def info_str(self):
		""" Should a specific message choose to add some more info to __str__ and __repr__ it can override this method """
		# the default message for human-readable representation is quite dull
		return "Payload type has no description renderer"

	def __str__(self):
		"""
			Subclasses are allowed to add some of their own to __str__ by exposing an info_str() method
		"""
		# note that the type qualifier is NOT counted in the size uint of the data
		info_str = self.info_str()

		return ("PostgreSQL protocol message: %s[%scomplete, total_size: %s, current_size: %d](%s)" % (
			self.__class__.__name__,
			("" if ((self.length is not None) and ((len(self.data) - self._qualifier_lenght) == self.length)) else "in"),
			(str(self.length) if (self.length is not None) else "<unknown>"),
			len(self.data),
			self.info_str()
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


	_PROTOCOL_VERSION_PACKER = struct.Struct("!hh")

	def decode_hook(self):
		""" Just extracts version information """
		(self.protocol_major, self.protocol_minor) = self._PROTOCOL_VERSION_PACKER.unpack(
			self.data[self._LENGTH_PACKER.size:self._LENGTH_PACKER.size + self._PROTOCOL_VERSION_PACKER.size]
		)

		# we break the connection string into pieces, and decode the contents
		opt_n = None
		for cnn_opt_w in self.data[self._LENGTH_PACKER.size + self._PROTOCOL_VERSION_PACKER.size:-1].split(b"\x00"):
			if (opt_n is None):
				opt_n = cnn_opt_w.decode()
			else:
				self.options[opt_n] = cnn_opt_w.decode()
				opt_n = None

		return True


	def encode_hook(self):
		""" Wrap this up. Absolutely untested """
		if (not (self.protocol_major and self.protocol_minor)):
			raise MessageEncodeError("Both protocol_major and protocol_minor must be set")

		# we just stick everything together
		return (
			self._PROTOCOL_VERSION_PACKER.pack(self.protocol_major, self.protocol_minor)
		+
			b"\x00".join((ok.encode() + b"\x00" + ov.decode() for (ok, ov) in self.options.items())) + (b"\x00" * 2)
		)


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
		self.messages = [(ml[:1].decode(), ml[1:].decode()) for ml in self.data[self.SIGNATURE_SIZE:-1].split(b"\x00\x00")]

		return True

	def encode_hook(self):
		if (not len(self.messages)):
			raise MessageEncodeError("There are no message strings")
		return b"\x00".join(((mh.encode() + mb.encode()) for (mh, mb) in self.messages)) + b"\x00\x00"
	
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

	
	_AUTH_MODE_PACKER = struct.Struct("!i")
	_SALT_PACKER = struct.Struct("!i")

	def encode_hook(self):
		
		if (self.mode is None):
			raise(MessageEncodeError("mode is not set"))

		if ((self.mode == AuthenticationMode.MD5Password) and (self.salt is None)):
			# we just generate one
			self.salt = random.randint(-2147483648, 2147483647)

		if ((self.mode == AuthenticationMode.GSSContinue) and (self.gss_sspi_data is None)):
			raise(MessageEncodeError("GSSAPI/SPI authentication data is required"))

		out_str = self._AUTH_MODE_PACKER.pack(self.mode)
		
		if (self.mode == AuthenticationMode.MD5Password):
			out_str += self._SALT_PACKER.pack(self.salt)
		elif (self.mode == AuthenticationMode.GSSContinue):
			out_str += self.gss_sspi_data

		return out_str

	# todo: decode hook for the backend-facing side
	def decode_hook(self):

		self.mode = self._AUTH_MODE_PACKER.unpack(self.data[self.SIGNATURE_SIZE:self.SIGNATURE_SIZE + self._AUTH_MODE_PACKER.size])[0]

		# we assume that GSS SSPI will always be at least 4 bytes
		min_size = (self.SIGNATURE_SIZE + self._AUTH_MODE_PACKER.size + 4)

		if (self.length < min_size):
			raise MessageDecodeError("Insufficient message size for Authentication (must be at least %d, is %d)" %
				min_size,
				self._LENGTH_PACKER.size + self.length
			)

		data_start = self.SIGNATURE_SIZE + self._AUTH_MODE_PACKER.size

		if (self.mode == AuthenticationMode.MD5Password):
			expected_size = self.SIGNATURE_SIZE.size + self._AUTH_MODE_PACKER.size + self._SALT_PACKER.size
			if (self.length != expected_size):
				raise MessageDecodeError("Wrong message size for MS5 authentication (must be %d, is %d)" % (
					expected_size,
					self._LENGTH_PACKER.size + self.length
				))

			self.salt = self._SALT_PACKER.unpack(self.data[data_start:data_start + self._SALT_PACKER.size])[0]

		elif (self.mode == AuthenticationMode.GSSContinue):
			self.gss_sspi_data = self.data[data_start:]
		else:
			raise MessageDecodeError("Unknown authentication mode: %d" % mode)
		return True


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
		self.password = self.data[self.SIGNATURE_SIZE:-1].decode()
		return True

	def encode_hook(self):
		if (not isinstance(self.password, bytes)):
			raise MessageEncodeError("Password property is not set/invalid")
		return self.password.encode()


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
	
	_PID_PACKER = struct.Struct("!i")


	def decode_hook(self):
		(self.backend_pid) = self._PID_PACKER.unpack(self.data[self.SIGNATURE_SIZE:self._PID_PACKER.size])
		self.secret_key = self.data[self.SIGNATURE_SIZE + self._PID_PACKER.size:self.SIGNATURE_SIZE + self._PID_PACKER.size + self._SECRET_KEY_PACKER.size]
		return True

	def encode_hook(self):
		return self._PID_PACKER.pack(self.backend_pid) + self.secret_key

	def info_str(self):
		return "process ID: %d, secret key: 0x%s" % (self.backend_pid, self.secret_key.hex())



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
		(self.parameter, self.value) = (ps.decode() for ps in self.data[self.SIGNATURE_SIZE:-1].split(b"\x00"))
		return True

	def encode_hook(self):
		return b"\x00".join((ps.encode() for ps in (self.parameter, self.value))) + b"\x00"
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
		self.query = self.data[self.SIGNATURE_SIZE:-1].decode()
		return True

	def encode_hook(self):
		if (self.query is None):
			raise MessageEncodeError("Query is not set")
		return self.query.encode()

	def info_str(self):
		return self.query.replace("\\n", "\\\\n")[0:32] if (self.query is not None) else "/* no query */"





class FieldDefinition():
	"""
		Helper class to properly describe the attributes of a data field, as
		understood by the protocol (see the RowDescription message type at
		https://www.postgresql.org/docs/current/static/protocol-message-formats.html )

		Differences from the format described in the document are that the type is
		a reference to a subclass of PGType and that the type length is not specified as
		it is dictated by the referenced class (it's -1 if the class does not have a packer
		structure)
	"""
	
	# the following only packs the numerical attributes, as the name is variable-length
	packer = struct.Struct("!ihihih")

	def __init__(self,
		name,
		data_type,
		rel_oid = 0,
		att_num = 0,
		type_mod = 0,
		is_binary = False
	):
		"""
			A very simple constructor that hydrates the members. See the document
			referenced in the class docstring for their definitions.

			All arguments straight-out turn into members.

			Note that the defaults pretty much specify what you'd get with "unknown"
		"""

		# any defined argument here becomes a member
		sl = locals().copy()
		if (not issubclass(data_type, pg_data.PGType)):
			raise TypeError("%s is not a subclass of PGType" % pg_type.__name__)

		[setattr(self, arg_in, sl[arg_in]) for arg_in in sl.keys()]


	@classmethod
	def from_buffer(cls, buffer):
		"""
			Decodes a field definition from a buffer.
			This definition format is kinda awkward to parse, as each field begins with a variable
			length string followed by packed values
		"""
		nullchar_offset = buffer.find(b"\x00")
		if (nullchar_offset < 0):
			raise MessageDecodeError("Field definition contains no nullchar")
		elif (nullchar_offset == 0):
			raise MessageDecodeError("Field name has no length")

		if ((len(buffer) - (nullchar_offset + 1)) != cls.packer.size):
			raise MessageDecodeError("Wrong number of packed values after nullchar")

		try:
			f_name = buffer[0:nullchar_offset].decode()
		except Exception as e_decode:
			raise ValueError("Error while decoding field name: %s(%s)" % (e_decode.__class__.__name__, e_decode))

		(reloid, attnum, typoid, typsize, typmod, isbinary) = cls.packer.unpack(buffer[nullchar_offset + 1:])

		if (typoid not in pg_data.OID_CLASSES):
			raise TypeError("Unknown type oid: %d" % typoid)
		
		return cls(
			name = f_name,
			data_type = pg_data.OID_CLASSES[typoid],
			rel_oid = reloid,
			att_num = attnum,
			type_mod = typmod,
			is_binary = bool(isbinary)
		)


	def __bytes__(self):
		""" Converts to the network representation """
		return self.name.encode() + b"\00" + self.packer.pack(
			self.rel_oid, self.att_num, self.data_type.oid,
			self.data_type.length, self.type_mod, int(self.is_binary)
		)

	def __str__(self):
		return "name = `%s`, type = %s(OID: %d, %s), rel_oid = %d, att_num = %d, type_mod = %d, encoded as %s" % (
			self.name,
			self.data_type.__name__,
			self.data_type.oid,
			("%d bytes" % self.data_type.length) if (self.data_type.length is not None) else "variable size",
			self.rel_oid,
			self.att_num,
			self.type_mod,
			"binary" if self.is_binary else "text"
		)

	def __repr__(self):
		return "< %s(%s) >" % (self.__class__.__name__, self.__str__())



class RowDescription(QualifiedMessage):
	"""
		Relatively complex message type, mostly because one the fields is
		variable-lenght (the field name).

		Could probably be implemented with a secondary inheritance from pg_data.RowDefinition,
		but we'd still have to maintain "fields" manually, so we proxy some of the
		methods in a protected fashion (to stop 

		The constructor allows for an iterable of fields to be passed. EG:

			rd = RowDescription([
				FieldDefinition(name = "foo"),
				FieldDefinition(name = "bar", data_type = PGBool)
			])

	"""
	ALLOWED_RECIPIENTS = PeerType.FrontEnd
	TYPE_QUALIFIER = b"T"
	# the "fields" is just an instance of the row description, empty by default
	PAYLOAD_MEMBERS = {
		"data_fields": []
	}
	AUTO_DECODE = False

	_FIELD_NUM_PACKER = struct.Struct("!h")


	def __init__(self, field_definitions):
		"""
			The constructor accepts a RowDefinition type, which is immutable
			We pre-hydrate the "fields" member with that list of FieldDefinitions
		"""
		super().__init__()
		self.data_fields.extend(field_definitions)
		

		
	def encode_hook(self):
		# Documentation says: column name, a nullchar and all the numerical attributes
		# packed as follows, the whole thing repeated for each column

		return self._FIELD_NUM_PACKER.pack(len(self.data_fields)) + b"".join(
			bytes(fld) for fld in self.data_fields
		)


	def decode_hook(self):
		# this requires some degree of smart parsing


		# first 2 bytes advertise field numbers.
		fld_num = self._FIELD_NUM_PACKER.unpack(self.data[self.SIGNATURE_SIZE:self.SIGNATURE_SIZE + 2])[0]

		if (fld_num < 0):
			raise MessageDecodeError("The buffer indicates a negative number of fields (%d)" % fld_num)
		
		# this definition format is kinda awkward to parse, as for each field we need to look for the
		# first nullchar (the null terminator of the name), unpack what follows and move the pointer ahead.
		# This involves more lenght sanity checks than it's healthy.
		# I'd bet money that the backend itself happily uses sscanf() for this

		total_length = len(self.data)
		fld_start = self.SIGNATURE_SIZE + 2 # "pointer" to the beginning of the next field definition
		for fld_id in range(0, fld_num, 1):
			nullchar_offset = self.data.find(b"\x00", fld_start)
			if (nullchar_offset < 0):
				raise MessageDecodeError("Could not locate name terminator for field #%d" % fld_id)

			fld_end = nullchar_offset + 1 + FieldDefinition.packer.size
			
			if (fld_end > total_length):
				raise MessageDecodeError("Not enough numeric attribute bytes after the field name at field #%d" % fld_id)

			# we also test encoding compliance explicitly
			try:
				self.data_fields.append(FieldDefinition.from_buffer(self.data[fld_start:fld_end]))
			except Exception as e_decode:
				raise MessageDecodeError("Unable to decode name string at field #%d - %s(%s)" % (fld_id, e_decode.__class__.__name__, e_decode))

			if (fld_end == total_length):
				# done
				break

			# me move onto the next one
			fld_start = fld_end
		
		return True

	def info_str(self):
		# We let the data class do the heavy lifting here
		return "%d fields: " % len(self.data_fields) + str(self.data_fields)

	def create_row(self, values = ()):
		"""
			Row formatting function. Simply expects an iterable of values.
			The supported input types for each member are None, bool, int, float, str, byte[array].
			They're translated into the closest approximation of an on-the-wire type representation
			that is defined in the row definition.

			Args:
				values:			an iterable of the values.
								The wrong number of members for this RowDefinition or any member that
								cannot be cast to the matching FieldDefinition cause an exception
			
			Return:
				a DataRow instance

		"""
		pass



# class DataRow(QualifiedMessage):
# 	"""
# 		One of these is sent off to the frontend for each data row in a set.
# 		It's not possible to use this class on its own to decode a record as
# 		from a buffer as a DataRow definition is necessary for that.
# 		
# 		The factory for these objects is RowDescription.decode_row()

# 		Rows are immutale
# 	"""
# 	ALLOWED_RECIPIENTS = PeerType.FrontEnd
# 	TYPE_QUALIFIER = b"D"
# 	# the "fields" is just an instance of the row description, empty by default
# 	PAYLOAD_MEMBERS = {
# 		"row_fields": []
# 	}
# 	AUTO_DECODE = False

# 	_FIELD_COUNT_PACKER = struct.Struct("!h")
# 	_FIELD_SIZE_PACKER = struct.Struct("!i")

# 	def __init__(self, fields):
# 		"""
# 			Accepts any iterable of PGTypes
# 		"""
# 		super().__init__()
# 		for fld in fields:
# 			if (not isinstance(fld, pg_data.PGType)):
# 				raise TypeError("All members of fields must be instances of PGType")
# 		self.fields = tuple(fields)

# 	def encode_hook(self):
# 		"""
# 			The type's __bytes___ do all the heavy lifting here
# 		"""
# 		out_cells = tuple(map(bytes, self.fields))

# 		return (
# 			self._FIELD_COUNT_PACKER.pack(len(out_cells))
# 		+
# 			b"".join(self._FIELD_SIZE_PACKER.pack(len(cell)) + cell for cell in out_cells)
# 		)


# 	def decode(self):
# 		"""
# 			Unlike most message types, it is impossible to decode 
# 		"""
# 		if (len(self.data) < self.SIGNATURE_SIZE + self._FIELD_COUNT_PACKER.size):
# 			raise MessageDecodeError("Insufficient bytes for a meaningful DataRow message")
# 		fld_num = self._FIELD_COUNT_PACKER.unpack(self.data[self.SIGNATURE_SIZE:self.SIGNATURE_SIZE + self._FIELD_COUNT_PACKER.size])[0]
# 		
# 		flds = []
# 		fld_size_pos = self.SIGNATURE_SIZE + self._FIELD_COUNT_PACKER.size
# 		for fld_id in range(0, fld_num):
# 			fld_width = 

# 		return True



################################################################
################################################################
### MODULE INITIALIZATION/Monkey patching
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
						raise AttributeError("Class %s is a subclass of QualifiedMessage which is active by defining ALLOWED_RECIPIENTS but does not define its TYPE_QUALIFIER member (must be a single byte)" % (msg_class.__name__))

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



x = RowDescription((FieldDefinition("frank", pg_data.PGBoolean),))
x.encode()
print(x)
