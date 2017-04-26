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

# generated from `grep define src/include/catalog/pg_type.h | grep OID ....`
BUILTIN_OIDS = {
	"BOOLOID": 16, "BYTEAOID": 17, "CHAROID": 18, "NAMEOID": 19, "INT8OID": 20, "INT2OID": 21, "INT2VECTOROID": 22, "INT4OID": 23, "REGPROCOID": 24, "TEXTOID": 25, "OIDOID": 26,"TIDOID": 27,
	"XIDOID": 28, "CIDOID": 29, "OIDVECTOROID": 30, "JSONOID": 114, "XMLOID": 142, "PGNODETREEOID": 194, "PGNDISTINCTOID": 3361, "PGDEPENDENCIESOID": 3402, "PGDDLCOMMANDOID": 32, "POINTOID": 600,
	"LSEGOID": 601, "PATHOID": 602, "BOXOID": 603, "POLYGONOID": 604, "LINEOID": 628, "FLOAT4OID": 700, "FLOAT8OID": 701, "ABSTIMEOID": 702, "RELTIMEOID": 703, "TINTERVALOID": 704, "UNKNOWNOID": 705,
	"CIRCLEOID": 718, "CASHOID": 790, "MACADDROID": 829, "INETOID": 869, "CIDROID": 650, "MACADDR8OID": 774, "INT2ARRAYOID": 1005, "INT4ARRAYOID": 1007, "TEXTARRAYOID": 1009, "OIDARRAYOID": 1028,
	"FLOAT4ARRAYOID": 1021, "ACLITEMOID": 1033, "CSTRINGARRAYOID": 1263, "BPCHAROID": 1042, "VARCHAROID": 1043, "DATEOID": 1082, "TIMEOID": 1083, "TIMESTAMPOID": 1114, "TIMESTAMPTZOID": 1184,
	"INTERVALOID": 1186, "TIMETZOID": 1266, "BITOID": 1560, "VARBITOID": 1562, "NUMERICOID": 1700, "REFCURSOROID": 1790, "REGPROCEDUREOID": 2202, "REGOPEROID": 2203, "REGOPERATOROID": 2204,
	"REGCLASSOID": 2205, "REGTYPEOID": 2206, "REGROLEOID": 4096, "REGNAMESPACEOID": 4089, "REGTYPEARRAYOID": 2211, "UUIDOID": 2950, "LSNOID": 3220, "TSVECTOROID": 3614, "GTSVECTOROID": 3642,
	"TSQUERYOID": 3615, "REGCONFIGOID": 3734, "REGDICTIONARYOID": 3769, "JSONBOID": 3802, "INT4RANGEOID": 3904, "RECORDOID": 2249, "RECORDARRAYOID": 2287, "CSTRINGOID": 2275, "ANYOID": 2276,
	"ANYARRAYOID": 2277, "VOIDOID": 2278, "TRIGGEROID": 2279, "EVTTRIGGEROID": 3838, "LANGUAGE_HANDLEROID": 2280, "INTERNALOID": 2281, "OPAQUEOID": 2282, "ANYELEMENTOID": 2283,"ANYNONARRAYOID": 2776,
	"ANYENUMOID": 3500, "FDW_HANDLEROID": 3115, "INDEX_AM_HANDLEROID": 325, "TSM_HANDLEROID": 3310, "ANYRANGEOID": 3831
}
# This is a dictionary we resolve OIDs into the relevant classes from (will be monkey-patched later)
OID_CLASSES = {}


class TypeEncodeError(Exception):
	pass
	
class TypeDecodeError(Exception):
	pass


class PGType():
	"""
		Generic base class to implement protocol representation of various SQL
		data type primitives.

		The PACK_STRING is actually used by other classes to monkey-patch several
		types into a combined record processor with a pre-compiled pack string when possible

		The byte-order is always bigendian.

		All types are immutable

		Multiple inheritance comes handy here

	"""
	OID_SYMBOL = None
	PACK_STRING = None

	oid = None
	packer = None
	pack = None
	unpack = None
	length = None


	# For some reason, the following composition breaks Decimal handling for PGNumeric
	#def __new__(cls, *args, **kwargs):

		#if ((cls.PACK_STRING is PGType.PACK_STRING) and (cls.pack is PGType.pack)):
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
			as-is for simple types. Types with special needs override it into
			something more peculiar
		"""
		return self.packer.pack(self)

	__bytes__ = pack
	

class PGBoolean(PGType, int):

	"""
		Bool cannot be subclassed, so we need to fake it in a very crude way.
		Better ideas are welcome
	"""
	OID_SYMBOL = "BOOLOID"
	PACK_STRING = "b"
	
	def __new__(cls, *args, **kwargs):
		"""
			This ensure that they can pass in whatever they want for boolean evaluation,
			much like whith bool
		"""
		return int.__new__(cls, 1 if (len(args) and args[0]) else 0)
	
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
	



class PGSmallInt(PGType, int):
	OID_SYMBOL = "INT2OID"
	PACK_STRING = "h"

class PGInt(PGType, int):
	OID_SYMBOL = "INT4OID"
	PACK_STRING = "i"

class PGBigInt(PGType, int):
	OID_SYMBOL = "INT8OID"
	PACK_STRING = "q"

class PGReal(PGType, float):
	OID_SYMBOL = "FLOAT4OID"
	PACK_STRING = "f"

class PGDouble(PGType, float):
	OID_SYMBOL = "FLOAT8OID"
	PACK_STRING = "d"
	
class PGFloat(PGDouble):
	pass


# Variable-length types below.
class PGNumeric(PGType, decimal.Decimal):
	OID_SYMBOL = "NUMERICOID"

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


# Text types
class PGText(PGType, str):
	OID_SYMBOL = "TEXTOID"
	"""
		TEXT. All other multibyte string types are just the same thing
	"""
	@classmethod
	def unpack(cls, buffer_data): return buffer_data.encode()
	def pack(self):	return self.encode()



class PGChar(PGText):
	OID_SYMBOL = "CHAROID"

class PGVarChar(PGText):
	OID_SYMBOL = "VARCHAROID"


# Binary types
class PGBytea(PGType, bytes):
	OID_SYMBOL = "BYTEAOID"
	"""
		This is already packed by definition
	"""
	def unpack(cls, buffer_data): return buffer_data
	def pack(self):	return self
	






################################################################
################################################################
### MODULE INITIALIZATION/Monkey patching
################################################################
################################################################

# we put together the packers for individual types
for (name, pg_type) in dict(sys.modules[__name__].__dict__.items()).items():
	# if the message has a non-empty type qualifier, we ensure that it is valid
	if (isinstance(pg_type, type)):
		if (issubclass(pg_type, PGType) and (pg_type is not PGType)):
			
			# populate the oid->class resolver and the oid
			if (pg_type.OID_SYMBOL is None):
				raise AttributeError("%s does not define an OID_SYMBOL")
			
			pg_type.oid = BUILTIN_OIDS[pg_type.OID_SYMBOL]
			OID_CLASSES[BUILTIN_OIDS[pg_type.OID_SYMBOL]] = pg_type


			# pack() might have been overridden, so we
			# always need to target __bytes__() at its current value
			setattr(pg_type, "__bytes__", pg_type.pack)

			# if the class has a custom pack() we don't touch it
			if (pg_type.pack is PGType.pack):
				if (pg_type.unpack is not PGType.unpack):
					raise AttributeError("%s overrides pack() but not unpack(). Cannot continue" % pg_type.__name__)
				if ((pg_type.PACK_STRING is PGType.PACK_STRING) or (len(pg_type.PACK_STRING) == 0)):
					raise AttributeError("%s neither overrides pack() nor PACK_STRING. Cannot continue" % pg_type.__name__)
				# There's a string to pack. We create the persistent packing object
				# and wrap the packing function around it
				pg_type.packer = struct.Struct("!" + pg_type.PACK_STRING)
				pg_type.length = pg_type.packer.size

			else:
				# this class overrode pack()
				if (not callable(pg_type.pack)):
					raise AttributeError("%s overrides pack() into a non-callable" % pg_type.__name__)



