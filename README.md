# PgPlex


pgplex is an HA postgresql connection proxy/aggregator/pooler which understand PosgreSQL layer 7 protocol messages and makes connection forwarding decisions based on the inferred session state/settings

# H2 Intended features

# H4 CORE

* PostgreSQL proxy listener
...Present any number of postgres clusters as if they were a single one, even across differen versions of the backend. This is done by allowing for the target cluster identifier to be encoded in the connection string's DB name,  thereby removing the need to provision a large number of addresses in your client facing network

* Quorim-controlled masetr proxy/arbitrator allows a number of pgplex instances to operate in HA.
...A decision has not been made yet about whether or not to we should rely on external tools such as etcd for this

* Session state-aware, nondisruptive transaction pooling
...It is implemented by restoring the backend state to the configuration the requesting client expects to find it in be before leasing the pooled connectin out.
...Ability to survive failover/loss of backend without resetting the downstream connection, therefore making it seamless to more naive clients that don't implement reset resilience (sessions that were in a transaction will receive a "transaction aborted" message from then on until they explicitly acknowledge the failed state by issuing a rollback)

* Traditional connection pooling
...This is just a special case whereby a given pool lease last until client disconnection
 
* Overlay pgplex-specific connection options to the standard Postgresql connection string..
... This allows a client to control the behavior of listener/multiplexer for a given session

* Shebang-like strings for command/transaction specific behavior control
... A specific comment format ```--!pgplex <arguments>``` allows a client to control the behavior of the service after a session has been established

* Management database to monitor state
... Much like modern unix-like systems expose the likes of /proc and /sys for a consisten abstrction model of their configuration, pgplex uses a reserved database name to abstract configuration/management. Since there is no actual PostgreSQL parser running there, the only supported commands are the aforementioned shebang notation and SET/RESET/SHOW


* Backend monitoring-based automatic failover
... pgplex reserves a backend superuser connection for each node for monitoring and configuration changes. This allows it to collect information and detect a server failure and trigger a failover. It is recommended to install the "pgplex" instrumentation schema, which allows for SQL-controlled failover, which is much safer than remote commands

# H4 EXTRA
* Dynamic LOAD balancing
... Since pgplex constantly monitors all configured backends through a dedicated connection, it is acutely aware of the state of each backend node/replication state/connection slots availability and can route connections based on that information.
... Actual load-aware balancing can be achieved if the "pglpex" schema is installed on the backends, as it presents system metrics through database objects
... A session can state its read-only/read-write intent through a special connection string option, or later during the session by prefixing its statements with a shebang-like string

