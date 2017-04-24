#!/usr/bin/python3 -uB
import sys
import logging
import struct
import decimal

LOGGER = logging.getLogger(__name__)



"""
Classes that facilitate the encoding of the layer7 representation of actual data
in the postgresql frontend-backend protocol. The reason this is implemented in
a separate module than pg_messages is that these objects do not translate into
full messages

Technically these are message fragments


Although that would work, this would be to slow to decode the in-band
traffic; it's meant to easily generate/accept records for/to the management
SQL interface.


"""

class TypeEncodeError(Exception):
	pass
	
class TypeDecodeError(Exception):
	pass


class PGScalar():
	"""
		Generic base class to implement protocol representation of various SQL scalar primitives.
		
		The PACK_STRING is actually used by other classes to monkey-patch several
		types into a combined record processor with a pre-compiled pack string when possible

		The byte-order is always bigendian.
		
		Multiple inheritance comes handy here

	"""
	PACK_STRING = None
	packer = None
	pack = None
	unpack = None
	

	# For some reason, the following composition breaks Decimal handling for PGNumeric
	#def __new__(cls, *args, **kwargs):

		#if ((cls.PACK_STRING is PGScalar.PACK_STRING) and (cls.pack is PGScalar.pack)):
			#raise AttributeError("Cannot instantiate %d directly as it does not override either PACK_STRING or pack()/unpack()")

		#return super().__new__(cls)

	@classmethod
	def from_buffer(cls, buffer_data):
		"""
			Allows the creation of a scalar from a buffer the size of the type
		"""
		try:
			if (cls.packer is not None):
				return cls(cls.packer.unpack(buffer_data)[0])
			else:
				return cls(cls.unpack(buffer_data))

		except Exception as e_decode:
			raise TypeDecodeError("Unable to decode %s from buffer: %s(%s)" % (cls.__name__, e_decode.__class__.__name__, e_decode))


	def pack(self):
		"""
			the default pack function tries to use the prepared packer and is left
			as-is for simple types
		"""
		return self.packer.pack(self)

	# this is just an alias now
	__bytes__ = pack
	

class PGBool(PGScalar, int):

	"""
		Bool cannot be subclassed, so we need to fake it in a very crude way.
		Better ideas are welcome
	"""
	PACK_STRING = "b"
	
	def __new__(cls, *argc, **argv):
		"""
			This ensure that they can pass in whatever they want for boolean evaluation,
			much like whith bool
		"""
		return int.__new__(cls, 1 if argc[0] else 0)
	
	# define some base methods for the next steps to work
	def __bool__(self):	return (self is not None) and self != 0
	__nonzero__ = __bool__

	# copy the goot ones from bool legit
	lv = locals().copy()
	for bool_m in bool.__dict__:
		if (bool_m not in lv):
			locals()[bool_m] = bool.__dict__[bool_m]

	def __str__(self): return str(self.__bool__())
	def __repr__(self): return str(self.__bool__())
	



class PGSmallInt(PGScalar, int):
	PACK_STRING = "h"

class PGInt(PGScalar, int):
	PACK_STRING = "i"

class PGBigInt(PGScalar, int):
	PACK_STRING = "q"

class PGReal(PGScalar, float):
	PACK_STRING = "f"

class PGDouble(PGScalar, float):
	PACK_STRING = "d"


# Variable-length types below.
class PGNumeric(PGScalar, decimal.Decimal):

	# the following are copied as-is from https://doxygen.postgresql.org/pgtypes__numeric_8h_source.html
	# and will have to be updated manually if need be
	NUMERIC_POS = 0x0000
	NUMERIC_NEG = 0x4000
	NUMERIC_NAN = 0xC000
	NUMERIC_NULL = 0xF000
	NUMERIC_MAX_PRECISION = 1000
	NUMERIC_MAX_DISPLAY_SCALE = NUMERIC_MAX_PRECISION
	NUMERIC_MIN_DISPLAY_SCALE = 0
	NUMERIC_MIN_SIG_DIGITS = 16


	# See the doctring of pack() about the following
	_PACK_HEADER = "hhhh"
	_PACK_QUARTET = "h"

	_HEADER_PACKER = struct.Struct("!" + _PACK_HEADER)
	_HEADER_SIZE = _HEADER_PACKER.size
	
	# we prepare a bunch of string packers for numerics up to 1024 total digits
	# (256 in total as the strings are always as long as some multiple of 4)
	# Indexed by the number of digit quartets (for base 10k) that we expect
	_NUM_STR_PACKERS = {}
	for pl in range(0, 256, 1):
		_NUM_STR_PACKERS[pl] = struct.Struct("!" + _PACK_HEADER + (_PACK_QUARTET * pl))



	@classmethod
	def unpack(cls, buffer_data):
		"""
			Specific decoder for Numeric
		"""
		
		if (len(buffer_data) < cls._HEADER_SIZE):
			raise TypeDecodeError("Insufficent data to decode a %s (%d bytes required, %d supplied)" % (cls.__name__, cls._HEADER_SIZE, len(buffer_data)))

		(ndigits, weight, sign, scale) = cls._HEADER_PACKER.unpack(buffer_data[0:cls._HEADER_SIZE])
		# some validation
		if (ndigits < 0):
			raise TypeDecodeError("The buffer specifies negative number of digits (%d)" % ndigits)

		if (sign == cls.NUMERIC_NAN):
			if (ndigits > 0):
				raise TypeDecodeError("The buffer specifies a number of digits greater than 0 was specified for a NaN value (%d)" % ndigits)


		if (len(buffer_data) != (cls._HEADER_SIZE + 2 * ndigits)):
			raise TypeDecodeError("Total buffer size (%d bytes) does not match the header size + ndigits * 2 (%d bytes)" % (len(buffer_data), (cls._HEADER_SIZE + 2 * ndigits)))

		# Time to decode. Since we want to use the same quick packers index we encode
		# with, we end up decoding the header again, which should be an acceptable
		# performance hit
		if (ndigits in cls._NUM_STR_PACKERS):
			upck = cls._NUM_STR_PACKERS[ndigits]
		else:
			upck = struct.Struct("!" + self._PACK_HEADER + (self._PACK_QUARTET * ndigits))

		all_fields = upck.unpack(buffer_data)
		
		# no other choice than manipulating a string here
		full_number = "".join(map(str, all_fields[4:]))
		if (weight < 0):
			full_number = "0." + "0" * (-(weight + 1)) + full_number
		else:
			# as per https://doxygen.postgresql.org/backend_2utils_2adt_2numeric_8c.html#a57a8f8ab552bae24926d252180956958,
			# we truncate some scale
			full_number = full_number[0:weight + 1] + "." + full_number[weight + 1:weight + 1 + scale]
			
		return full_number


	def pack(self):
		"""

			Encoded according to the structure Tom himself speaks of at
			https://www.postgresql.org/message-id/16572.1091489720%40sss.pgh.pa.us
			
			Encoding and decoding a postgres numeric is somewhat complicated: each
			16 bit word of the buffer encodes 4 digits of the full number (Thereby
			wasting relatively more positive type range that using individual chars
			would. Not sure why it's like that).


			Packet format is:

			ndigits -	total number of "4 digit" base 10k digits:		2 bytes
			weight -	weight of the first digit (base 10):			2 bytes
			sign -		sign/nan/null flags:							2 bytes
			scale -		number of digits to the right of the dot:		2 bytes
			digits -	the digit buffer, packed as described above:	2 * ndigits bytes

		"""
		# we always operate on a normalized value
		(dec_sign, dec_number, dec_exponent) = self.normalize().as_tuple()

		weight = len(dec_number) + dec_exponent - 1
		scale = len(dec_number) - weight - 1

		# Decimal returns an "n" for NaN and the like
		if (not (isinstance(dec_exponent, int))):
			sign = NUMERIC_NAN
		else:
			sign = self.NUMERIC_NEG if (dec_sign > 0) else self.NUMERIC_POS

		# we arithmetically build up those numbers.
		# Not sure if the fact I'm using a division makes it slower than simple str() mangling
		quartets = []
		for digit_figure in range(0, len(dec_number), 4):
			next_4d = 0
			for fig_off in range(0, 4):
				next_4d += int(1000 / 10 ** fig_off) * (dec_number[digit_figure + fig_off]) if ((digit_figure + fig_off) < len(dec_number)) else 0
			quartets.append(next_4d)


		# now we look into our list of pre-built packer objects
		if (len(quartets)) in self._NUM_STR_PACKERS:
			pck = self._NUM_STR_PACKERS[len(quartets)]
		else:
			pck = struct.Struct("!" + self._PACK_HEADER + (self._PACK_QUARTET * len(quartets)))


		return pck.pack(len(quartets), weight, sign, scale, *quartets)
			
		


	# This is weird: it looks like Decimal resolves to None when its __bytes__is accessed,
	# so we need to locally override it
	__bytes__ = pack


#class PGText(PGScalar, str):
	#def pack(self):	return str.encode()


# Non packable strings follow
#class PGC(PGScalar, str):
	#def pack(self):	return str.encode()



class FieldDefinition():
	"""
		Helper class to properly describe the attribute of a data field, as
		understood by the protocol (see the RowDescription message type at
		https://www.postgresql.org/docs/current/static/protocol-message-formats.html )
	"""

	def __init__(self,
		name,
		rel_oid = 0,
		att_num = 0,
		type_oid = 0,
		type_len = -1,
		type_mod = 0,
		format_code = 0
	):
		"""
			A very simple constructor that hydrates the members. See the document
			referenced in the class docstring for their definitions.
			
			All arguments straight-out turn into members.
			
			Note that the defaults pretty much specify what you'd get with "unknown"
		"""

		# any defined argument here becomes a member
		sl = locals().copy()
		[setattr(self, arg_in, sl[arg_in]) for arg_in in sl.keys()]

	def __str__(self):
		return "name = `%s`, rel_oid = %d, att_num = %d, type_oid = %d, type_len = %d, type_mod = %d, format_code = %d" % (
			self.name, self.rel_oid, self.att_num, self.type_oid, self.type_len,
			self.type_mod, self.format_code
		)

	def __repr__(self):
		return "< %s >" % self.__str__()


	
class RowDefinition(list):
	"""
		Phython lists come in handy here, as a row definition is just that:
		a list of FieldDefinitions.

		The overrides are there to stop wrong types
		
		Ideal instantiation example:

		x = RowDescription(RowDefinition([
			FieldDefinition(name = "foo"),
			FieldDefinition(name = "bar", type_len = 1234),
		]))

	"""
	ACCEPTED_CLASS = FieldDefinition

	def validate_member(self, value):
		"""
			Simply throw an exception if they try to add an member of an unsupported
			type
			
			Args:
				value:	what needs to be validated
				
			Return:
				always true, as it throws an exception when validation fails
		"""
		if (not isinstance(value, self.ACCEPTED_CLASS)):
			raise TypeError("%ss only accepts `%s` instances as their members. `%s` was supplied" % (self.__class__.__name__, self.ACCEPTED_CLASS.__name__, value.__class__.__name__))
			return True

	def append(self, value):
		self.validate_member(value)
		return super().append(value)

	def insert(self, index, value):
		self.validate_member(value)
		return super().insert(index, value)

	def extend(self, value):
		[self.validate_member(member) for member in value]
		return super().extend(value)
	
	def __iadd__(self, value):
		[self.validate_member(member) for member in value]
		return super().__iadd__(value)

	def __str__(self):
		return "%d columns(%s)" % (len(self), "), (".join(map(str, self)))

	def __repr__(self):
		return "< %s >" % self.__str__()





################################################################
################################################################
### MODULE INITIALIZATION/Monkey patching
################################################################
################################################################

# we put together the packers for individual types
for (name, pg_type) in dict(sys.modules[__name__].__dict__.items()).items():
	# if the message has a non-empty type qualifier, we ensure that it is valid
	if (isinstance(pg_type, type)):
		if (issubclass(pg_type, PGScalar) and (pg_type is not PGScalar)):
			# if the class has a custom pack() we don't touch it
			if (pg_type.pack is PGScalar.pack):
				if (pg_type.unpack is not PGScalar.unpack):
					raise AttributeError("%s overrides pack() but not unpack(). Cannot continue" % pg_type.__name__)
				if ((pg_type.PACK_STRING is PGScalar.PACK_STRING) or (len(pg_type.PACK_STRING) == 0)):
					raise AttributeError("%s neither overrides pack() nor PACK_STRING. Cannot continue" % pg_type.__name__)
				# There's a string to pack. We create the persistent packing object
				# and wrap the packing function around it
				pg_type.packer = struct.Struct("!" + pg_type.PACK_STRING)
			if (not callable(pg_type.pack)):
				raise AttributeError("%s overrides pack() into a non-callable" % pg_type.__name__)

