#!/usr/bin/python3 -uB
import logging

LOGGER = logging.getLogger(__name__)



"""
Classes that facilitate the encoding of the layer7 representation of data
in the postgresql frontend-backend protocol.

Technically these are message fragments

"""


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
		return "Field(name = `%s`, rel_oid = %d, att_num = %d, type_oid = %d, type_len = %d, type_mod = %d, format_code = %d)" % (
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
		""" Simply throw an exception if they pass rubbish """
		if (not isinstance(value, self.ACCEPTED_CLASS)):
			raise TypeError("%ss only accepts `%s` instances as their members. `%s` was supplied" % (self.__class__.__name__, self.ACCEPTED_CLASS.__name__, value.__class__.__name__))

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
		return "[%s]" % ",".join(map(str, self))

	def __repr__(self):
		return "< %s >" % self.__str__()
