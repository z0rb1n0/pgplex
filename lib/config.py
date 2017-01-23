#!/usr/bin/python3 -uB

"""
	Configuration manager for pgplex
	It's a silly JSON
"""
import os
import json
import logging
import configparser
import re

import info


# The following is a wild guess based on the relative path to the librariy's own
PGPLEX_BASE_PATH = os.path.realpath(os.path.dirname(__file__) + "/..")
PGPLEX_CONFIG_PATH = PGPLEX_BASE_PATH + "/etc"
PGPLEX_SSL_PATH = PGPLEX_CONFIG_PATH + "/ssl"



LOGGER = logging.getLogger(__name__)


CONFIG_DIR_RELATIVE = "." + info.APP_NAME

# Grand Unified Configuration, Postgres style, only with a 1-level directory system to boot
# 
# GUC_DEFINITIONS defines what settings the system supports
# 
# Each level key here contains an 1-to-3, tuple (2nd 3rd and 4th member are optional)
# Tuple members:
# 1) Expected type (python type. Cast is attempted for validation)
# 2) Accepted values.
#		For integers/floats 1-2 member tuple specifying inclusive bonds is expected (upper bound set to the lower not unspecified)
#    	For string a it can be either a tuple with the set of accepted values or a compiled regular expression
#    Can be None, in which case no validation is performed
# 3) Default value.
# 		For settings with arbitrary values it's the actual value (None if not specified).
# 		For multiple-choice settings it is the index in the options list
# 4) Description of the setting. Defaults to None

# Example:
# GUC_DEFINITIONS = {
#  "barista": {
#   "name": (str, None, "Frank", "Just how [s]he's called"),
#   "accept_tips": (bool, None, True, "U.S. are weird"),
#   "shot_ratio": (float, (0.2, 0.4), 0.3, "Coffee/Total fraction"),
#   "greeting_line": (str, ( "Hello, what would you like today?", "What's your poison?", "Hello, how can I help you?" ], 1, "What [s]he says"),
#   "home_page": (str, re.compile("http://[.]*"), None, "Where [s]he's at")
#  }
# }

GUC_DEFINITIONS = {
	"global": {
		"config_file": (str, None, PGPLEX_CONFIG_PATH + "/pgplex.conf", "Path of the configuration file. As a matter of fact, can only be affected from the command line or environment (the former overrides the latter)"),
		"control_db_name": (str, None, "pgplex_control", """
			A special database name that can is reserved for connections to the pgplex run-time interface.
			Connection to this database cause queries to be routed to an internal management engine to
			inspect/manipulate the run-time state of pgplex.
		""")
	},
	"listener": {

		"listen_addresses": (str, None, "localhost", "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"port": (int, (1, 65535), 5432, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"max_connections": (int, (0, ), 3, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS . Note that this applies to incoming connections and does not encroach shared memory"),
		"max_connections_control_db": (int, (1,), 8, "much like max_connections, but it limits connections to the control_db instead. NOTE: connections to the system db count towards the global max_connections limit"),

		"unix_socket_directories": (str, None, "/tmp", "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"unix_socket_group": (str, None, None, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"unix_socket_permissions": (str, None, None, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),

		"tcp_keepalives_idle": (int, (0,), 0, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"tcp_keepalives_interval": (int, (0,), 0, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"tcp_keepalives_count": (int, (0,), 0, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),


		"ssl": (bool, None, False, "postgresql-equivalent, see https://www.postgresql.org/docs/9.6/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SECURITY"),
		"ssl_key_file": (str, None, PGPLEX_SSL_PATH + "/server.key", "postgresql-equivalent, see https://www.postgresql.org/docs/9.6/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SECURITY"),
		"ssl_ca_file": (str, None, PGPLEX_SSL_PATH + "/ca.crt", "postgresql-equivalent, see https://www.postgresql.org/docs/9.6/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SECURITY")

	},
	"multiplexer": {
		"channel_timeout": (int, (-1,), 0, """
			Transaction poolers continuously re-assign database sessions to diffent clients, however SQL sessins are stateful,
			which requires some degree of persistent correlation between the incoming connection and the backend
			(eg: a backend cannot safely be re-assigned to another client as long as it is in transaction).

			Clients also have the ability to change run-time parameters such as the session time zone (eg, through SET option TO ...,
			connection parameters or the like).	PgPlex keeps track of what postgres settings a given client configured for itself
			and restores them into to the server session before making the backend available to the client.
			This operation, however, comes at the cost of one round-trip to the postgres backend, which has a non negligible overhead,
			especially for workloads comprised of lots of small queries (obviously each query bind the client to the backend
			for its duration)

			To mitigate this problem, it is possible to cause client-backend bond (called "channel") to linger for some time after
			its query/transaction has ended, in order to increase the chances that the same binding will be reused without
			requiring settings to be restored, as the server-side parameters are guaranteed to be consistent with the client's
			expectations of them until unbind. Unbind_delay specifies such duration. Be advised that higher values are more likely
			to impact the multiplexer's ability to efficiently spread requests across the pool by reusing backends

			A value of 0 completely disables delayed unbinding, effectively forcing the backend run-time to be restored in full for each new query/transaction.
			A positive value specifies for how long a backend-facing connection should stay bound to the client after a query/transaction completion
			A vaule of -1 (the default) never unbinds client sessions from backends until they disconnect, effectively disabling transaction pooling and turning it into simple connection pooling

		""")
	},
	"pool": {
		"max_backends":	(int, (0,), 64, """ Maximum number of upstream connections to the upstream backends (typically an ac postgresql listeners) """)
	}
}




def get_guc_defaults():
	out_guc = {}
	# we first initialize a GUC with the default settings
	for section_name, section_settings in GUC_DEFINITIONS.items():
		out_guc[section_name] = {}
		for (setting_name, setting_def) in section_settings.items():
			# strings have a different treatment if there are conditional options
			if ((setting_def[0] is str) and isinstance(setting_def[1], (tuple, list))):
				# we pick the member in the multiple options
				out_guc[section_name][setting_name] = setting_def[0](setting_def[1][setting_def[2]])
			else:
				out_guc[section_name][setting_name] = setting_def[0](setting_def[2])
	return out_guc



def get_guc_file(cfg_file):
	"""
		Loads the configuration file and returns a nested dictionarly with the
		same keys as the guc, but only showing the keys that ARE set in the
		configuration file.
		
		Raises an exception on any error
		
		Args:
		
		cfg_file:				(str)The path of the config file
		
		
		Return value:
			the configuration array (ideally used to overlay it with the guc)
	"""
	out_guc = {}
	guc_fp = open(cfg_file, "r")
	cfg = configparser.ConfigParser()
	cfg.readfp(guc_fp)
	for sec_name in cfg.sections():
		if (sec_name in (GUC_DEFINITIONS)):
			out_guc[sec_name] = {}
			for key_name, key_value in cfg[sec_name].items():
				if (key_name in GUC_DEFINITIONS[sec_name]):
					out_guc[sec_name][key_name] = cfg[sec_name][key_name]
				else:
					LOGGER.warning("Unknown configuration option `%s.%s` in `%s`" % (sec_name, key_name, cfg_file))
		else:
			LOGGER.warning("Unknown configuration section `%s` in `%s`" % (sec_name, cfg_file))
	guc_fp.close()
	return out_guc
	
	
def get_guc_env():
	"""
		Generates a subset of the GUC tree from a set of known environment variables
	"""
	out_guc = {}
	return out_guc


def get_guc_cmdline():
	"""
		Generates a subset of the GUC tree from the command line arguments
	"""
	out_guc = {}
	return out_guc


def get_guc():
	"""
		Overlays configuration parameters in the correct order and returns
		the configuration (generally, they're overwritten in the following order:
		hardcoded_default, config_file, environment, command_line
		
		Returns the final configuration dictionary (which would be the new GUC).
		
		Also validates the configuration after the merger
		
		Raises exceptions at any failure
	"""
	out_guc = {}
	# we now loop through the 4 functions to load the loop, in the order we want
	# settings to be applied
	for guc_f in (
		get_guc_defaults,
		get_guc_file,
		get_guc_env,
		get_guc_cmdline
	):
		# we conditionally prepare an argument list for the GUC loaders that require one.
		guc_f_args = ({"cfg_file": out_guc["global"]["config_file"]} if (guc_f == get_guc_file) else {})
		s_guc = guc_f(**guc_f_args)
		# recursive merge of the array
		for (guc_section, guc_keys) in s_guc.items():
			if (guc_section not in (out_guc)):
				out_guc[guc_section] = {}
			for (guc_key, guc_value) in (guc_keys.items()):

				target_type = GUC_DEFINITIONS[guc_section][guc_key][0]
				
				# we now try to validate stuff. type cast first
				try:
					final_val = target_type(guc_value)
				except ValueError:
					raise ValueError("Option `%s.%s` expects values of type `%s`. Set value `%s` (from %s) is invalid" % (
						guc_section, guc_key, target_type.__name__, guc_value, guc_f.__name__
					))
					return None


				# is there any check on the contents?
				if (GUC_DEFINITIONS[guc_section][guc_key][1] is not None):
					# yup. Checks depend on the type and specified parameters.
					# we try to work them out
					
					if (target_type in (int, float)):
						# we assume that GUC_DEFINITIONS is not broken
						bond_l = target_type(GUC_DEFINITIONS[guc_section][guc_key][1][0])
						bond_u = target_type(GUC_DEFINITIONS[guc_section][guc_key][1][1]) if (len(GUC_DEFINITIONS[guc_section][guc_key][1]) > 1) else None
						num_format = ("%.9f" if (target_type is float) else "%d")
						if ((final_val < bond_l) or ((bond_u is not None) and (final_val > bond_u))):
							raise ValueError("Invalid value for `%s.%s`: `%s` (from %s) (must be between %s and %s)" % (
								guc_section, guc_key, (num_format % (final_val,)), guc_f.__name__,
								((num_format % (bond_l,)) if (bond_l is not None) else "-infinity"),
								((num_format % (bond_u,)) if (bond_u is not None) else "+infinity")
							))

				# multiple choices or regular expressions...
				if (target_type is str):
					str_validator = GUC_DEFINITIONS[guc_section][guc_key][1]
					if (isinstance(str_validator, (tuple, list))):
						if (final_val not in (str_validator)):
							raise ValueError("Invalid value for `%s.%s`: `%s` (from %s) (accepted values are `%s`)" % (
								guc_section, guc_key, final_val, guc_f.__name__, "`, `".join(str_validator)
							))
					

				out_guc[guc_section][guc_key] = guc_value
	
	return out_guc


