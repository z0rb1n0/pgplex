#!/usr/bin/python3 -uB

"""
	Configuration manager for pgplex
	It's a silly JSON
"""
import os
import json
import logging
import logging.config

import info


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

GUC_DEFINITIONS = {
	"global": {
		"control_db_name": (str, None, "pgplex_control", """
			A special database name that can is reserved for connections to the pgplex run-time interface.
			Connection to this database cause queries to be routed to an internal management engine to
			inspect/manipulate the run-time state of pgplex.
		""")
	},
	"listener": {

		"listen_addresses": (str, None, "localhost", "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"port": (int, (1, 65535), 5432, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"max_connections": (int, (0,), 1024, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS . Note that this applies to incoming connections and does not encroach shared memory"),
		"max_connections_control_db": (int, (1,), 8, "much like max_connections, but it limits connections to the control_db instead. NOTE: connections to the system db count towards the global max_connections limit"),

		"unix_socket_directories": (str, None, "/tmp", "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"unix_socket_group": (str, None, None, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"unix_socket_permissions": (str, None, None, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),

		"tcp_keepalives_idle": (int, (0, ), 0, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"tcp_keepalives_interval": (int, (0, ), 0, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		"tcp_keepalives_count": (int, (0, ), 0, "postgresql-equivalent, see https://www.postgresql.org/docs/current/static/runtime-config-connection.html#RUNTIME-CONFIG-CONNECTION-SETTINGS"),
		
		
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


guc = {}
# we first initialize a GUC with the default settings
for section_name, section_settings in GUC_DEFINITIONS.items():
	guc[section_name] = {}
	for (setting_name, setting_def) in section_settings.items():
		guc[section_name][setting_name] = setting_def[0](setting_def[2])

