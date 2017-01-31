# Project in early develoment. At the moment it is a glorified postgres protocol echo server




# PgPlex



pgplex is an HA postgresql connection proxy/aggregator/pooler which understand PosgreSQL layer 7 protocol messages and makes connection forwarding decisions based on the inferred session state/settings.

It is designed to use the SQL connection to the backends as the sole control channel (sometimes going as far as permanently incapacitating a stray primary DB via SQL). No node-to-node SSH required

---
---

## Planned features (will add a checklist with completion indicators soon).
_We're probably too ambitious and suck too much, but it's going to be a lot of work anyway so might as well go the extra mile..._


### Attainable with a vanilla PostgreSQL installation and no superuser access (as of 9.6)

* #### PostgreSQL proxy listener
 * Present any number of postgres clusters as if they were a single one, even across differen versions of the backend. This is done by allowing for the target cluster identifier to be encoded in the connection string's DB name,  thereby removing the need to provision a large number of addresses in your client facing network

* #### Quorim-controlled master proxy/arbitrator allows a number of pgplex instances to operate in HA.
 * A decision has not been made yet about whether or not to we should rely on external tools such as etcd for this

* #### Session state-aware, nondisruptive transaction pooling
 * It is implemented by restoring the backend state to the configuration the requesting client expects to find it in be before leasing the pooled connectin out.
 * Ability to survive failover/loss of backend without resetting the downstream connection, therefore making it seamless to more naive clients that don't implement reset resilience (sessions that were in a transaction will receive a "transaction aborted" message from then on until they explicitly acknowledge the failed state by issuing a rollback)

* #### Traditional connection pooling
 * This is just a special case whereby a given pool lease last until client disconnection
 
* #### Overlay pgplex-specific connection options to the standard Postgresql connection string..
 *  This allows a client to control the behavior of listener/multiplexer for a given session

* #### Shebang-like strings for command/transaction specific behavior control
 * A specific comment format ```--!pgplex <arguments>``` allows a client to control the behavior of the service after a session has been established

* #### Management database to monitor state
 * Much like modern unix-like systems expose the likes of /proc and /sys for a consisten abstrction model of their configuration, pgplex uses a reserved database name to abstract configuration/management. Since there is no actual PostgreSQL parser running there, the only supported commands are the aforementioned shebang notation and SET/RESET/SHOW


* #### Connections count/state aware load balancing
 * Since pgplex constantly monitors all configured backends through a dedicated connection, it is acutely aware of session count/state through constant monitoring of pg_stat_activity too. It can therefore make load-balancing decisions based on that information
 * To facilitate the handling of write-to-one-read-from-many access patterns, a session can state its read-only/read-write intent through a special connection string option, or later during the session by prefixing its statements with a shebang-like string

* #### Rate throttling
 * Although it is already possible to limit/throttle backend resource hogging through OS facilities (niceness/ulimit/control groups/jails) all of these limiting capabilities are unaware of database specifics. By virtue of independently handling protocol messages, pgplex should be able to limit queries-per-second/records fetched per second in an user/session/db specific manner (eg: useful for application optimization during development)

---

## Requiring support from server-side extension/procedural languages and/or superuser access

* #### Backend monitoring-based automatic promotion + failover
 *  pgplex reserves a backend superuser connection for each node for monitoring and configuration changes. This allows it to collect information and detect a server failure and trigger a failover by using privileged SQL code to promote the backend. It is recommended to install the "pgplex" instrumentation schema for this

* #### Dynamic, resource-based LOAD balancing
 * It is possible to use procedural languages/extensions to access OS statistics via SQL (we already have in the past, at least on Linux). The inferred load can be integrated to pg_stat_activity for better load balancing decisions

* #### md5-based validation on the proxy
 * Given that the monitoring/administration session can belong to a superuser, there is nothing stopping pgplex from using the backend's pg_authid for validation

* #### Enforcement of backend-mandated pg_hba.conf ACLs to the proxy
 * This requires pgplex to read the node's own pg_hba.conf. In principle this would be attainable without any superuser privilege/procedural language as pg_hba.conf normally resides in PGDATA and can therefore be read with pg_read_file(), but that is not always the case

* #### Propagation of every node's SSL keys/certificates the proxy
 * Same rules as the pg_hba.conf propagation apply
