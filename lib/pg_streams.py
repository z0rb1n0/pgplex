#!/usr/bin/python3 -uB
import guc
import sys
import logging
import ipc_streams
import enum
#from enum import Enum, auto


LOGGER = logging.getLogger(__name__)


# this is a quick way we use to resolve a message 
MESSAGE_TYPE_CLASSES = {}


class MessageTargets(enum.Enum):
	"""
		Possible directions a message type is expected to go (indicates destination).
		To be used as a bitmask
	"""
	Neither = 0 # Placeholder for Initialization
	Backend = 1
	Frontend = 2
	Both = 3


class PgBackendState(enum.Enum):
	"""
		These are possible states a postgres backend,
		and by extension a pgplex DownStreamSession, can be in.
		With the exception of states pertaining the initialization sequence,
		and the "busy" states, the value of the enum key is the message type
		indicator byte
	"""
	
	# this happens only at startup
	WaitingForUnqualifiedMessage = None
	WaitingForStartup = None


class UnqualifiedStreamMessage(object):
	"""
		For historical reasons, startup messages in postgres have no qualifier
		byte. They'll be subclassed directly from this class
		StartupMessage, SSLRequest, CancelRequest
	"""
	
	# this class member is for validation
	allowed_targets = MessageTargets.Neither
	
	def __init__():
		
		# this is the m
		self.type = None
		
		# total lenght of the message, excluding the type qualifier if there
		# is one
		self.length = None


class StreamMessage(UnqualifiedStreamMessage):
	"""
		Base class for all the modern postgres message that have types marker bytes
		at the beginning (the vast majority).
		
		https://www.postgresql.org/docs/current/static/protocol-message-formats.html
	"""
	# At module initialization, this class member is meant to be enumerated for all
	# the subclasses of this class and put into a dictionary for quick type resolution
	type_qualifier = b"\x00" # this is not valid



class CommandComplete(StreamMessage):
	allowed_targets = MessageTargets.Frontend
	type_qualifier = b"C"


# we build the message class resolver now
for (name, msg_class) in dict(sys.modules[__name__].__dict__.items()).items():
	if ((isinstance(msg_class, type)) and issubclass(msg_class, StreamMessage) and (msg_class is not StreamMessage)):
	#if (issubclass(thing, StreamMessage)):
		if (msg_class.type_qualifier == StreamMessage.type_qualifier):
			raise AttributeError("Class %s is a subclass of StreamMessage but does override its type_qualifier" % (msg_class.__name__))
		if (len(msg_class.type_qualifier) != 1):
			raise ValueError("Invalid type_qualifier lenght for class %s" % (msg_class.__name__))

		MESSAGE_TYPE_CLASSES[msg_class.type_qualifier] = msg_class



class DownStreamSession(ipc_streams.Stream):
	"""
		Handler of the process + connection talking to the actual frontend.
	"""
	def __init__(self,
		ds_sock
	):


		# downstreams sessions are always inbound
		super().__init__(ds_sock = ds_sock, outbound = False)
	
	
	def get_next_message(self):
		"""
			Blocks until it managed to buffer the entirety of the next message.

			

		"""
	
