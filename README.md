# Vert.x Cassandra Persistor [![Build Status](https://travis-ci.org/nea/vertx-mod-cassandra-persistor.svg?branch=master)](https://travis-ci.org/nea/vertx-mod-cassandra-persistor)
This very simple [Vert.x][1] module allows to store and retrieve data from a [Cassandra][2] instance or cluster. It uses the [DataStax Java Driver 2.1][3] (*defaults and characteristics from the driver are implicitly used*).

It is loosely based on the Vert.x [MongoDB persistor][4] and not optimized for highest performance but straight-forward integration with Cassandra. 

* Module `com.insanitydesign~vertx-mod-cassandra-persistor~X.X.X`
* Worker Verticle
* EventBus JSON-based
* CQL3

## Latest Versions
* 0.4.1
	* Disabled multi-threaded flag from mod.json
* 0.4.0
	* Added batched actions handler, processing an array of actions
* 0.3.6
	* Added additional age handling on cached statements for optimized overflow replacement

For a full version history go [here][5].

## Dependencies
This Persistor has only been developed and tested with Cassandra 2.x/CQL3. To use it you of course have to have a Cassandra instance running and accessible through the network, where this Module is running.

### Repository
The module is available through the central Maven Repository

    <dependency>
        <groupId>com.insanitydesign</groupId>
        <artifactId>vertx-mod-cassandra-persistor</artifactId>
        <version>0.4.1</version>
    </dependency>

## Configuration
The Vert.x Cassandra Persistor takes the following configuration

    {
        "address": <string>,
        "hosts": [<string>],
        "port": <int>,
        "keyspace": <string>,
        "compression": "SNAPPY" | "LZ4",
        "retry": "fallthrough" | "downgrading",
        "reconnection": {
            "policy": "constant" | "exponential",
            "delay": <int>,
            "max": <int>
        },
        "credentials": {
            "username": <string>,
            "password: <string>
        },
        "ssl": <boolean>,
        "fetchSize": <int>,
        "dateFormat": <string>,
        "prepStmtCacheSize": <int>,
        "batchTimeout": <int>
    }

An exemplary configuration could look like

    {
        "address": "your.awesome.persistency",
        "hosts": ["192.168.172.33", "192.168.172.34"],
        "port": 9142,
        "keyspace": "yourkeyspace"
    }

### Fields
* `address` *optional* The main address for the module. Every module has a main address. Defaults to `vertx.cassandra.persistor`
* `hosts` *optional* A string array of host IPs the module connects to as contact points. Defaults to `127.0.0.1`
* `port` *optional* The port (number) the Cassandra instances are running on. All hosts must have Cassandra running on the same port. Defaults to `9042`
* `keyspace` *optional* The Cassandra keyspace to use. Defaults to `vertxpersistor`. 
* `compression` *optional* Set the cluster connections compression to `SNAPPY` or `LZ4`. Defaults to `NONE`
* `retry` *optional* Set the cluster connections retry policy to `DowngradingConsistencyRetryPolicy` or `FallthroughRetryPolicy`. Defaults to `Policies.defaultRetryPolicy()`. See the drivers [JavaDoc][6] for more information.
* `reconnection` *optional* Set the cluster connections reconnection policy to `ConstantReconnectionPolicy` or `ExponentialReconnectionPolicy` (*exponential* requires `delay` and `max`). Defaults to `ConstantReconnectionPolicy`. See the drivers [JavaDoc][7] for more information.
* `credentials` *optional* A JsonObject containing the *username* and *password* to authenticate at the Cassandra hosts. Defaults to no credentials, expecting an *AllowAll* rule at the cluster.
* `ssl` *optional* Connect via SSL or not. Defaults to not.
* `fetchSize` *optional* The default fetch size for *SELECT* queries. Defaults to 5000.
* `dateFormat` *optional* The default Date pattern used to convert string dates to `Date` instances. Defaults to `dd-MM-yyyy HH:mm:ss`.
* `prepStmtCacheSize` *optional* The default prepared statement cache size used to store and manage prepared statements. Defaults to `Integer.MAX_VALUE`
* `batchTimeout` *optional* The timeout in milliseconds after which the batched queries are skipped to guarantee a return. Defaults to `30000`

## Operations

### Raw
*Please use with care!*

    {
        "action": "raw",
        "statement": <cql3Statement> | "statements": [<cql3BatchStatements>, ...]
    }
    
An example could look like

    {
        "action": "raw",
        "statement": "SELECT * FROM superkeyspace.tablewithinfos"
    }
    
or

    {
        "action": "raw",
        "statements": [
            "INSERT INTO superkeyspace.tablewithinfos (key, value) VALUES('Key 1', 'Value 1')",
            "INSERT INTO superkeyspace.tablewithinfos (key, value) VALUES('Key 2', 'Value 2')"
        ]
    }
    
where `statement` is preferred over `statements`.
    
#### Fields
`statement` A Cassandra Query Language version 3 (CQL3) compliant query that is channeled through to the driver and Cassandra.  
`statements` A JsonArray of Cassandra Query Language version 3 (CQL3) compliant queries that are channeled through to the driver and Cassandra. Only `UPDATE`, `INSERT` and `DELETE` are allowed.

*Note: Do not forget the keyspace (e.g. `FROM keyspace.table`), even if configured, as the raw statements are not altered in any way! And use `'` instead of `"` for strings.*

#### Returns
The `raw` action returns a `JsonArray` of `JsonObject`s in the format `columnName:columnValue` (if any result is given).
*Note: Value types are not fully interpreted but generally covered as numbers, strings or collections. Complex Types are not handled at the moment!*

### Prepared

    {
        "action": "prepared",
        "statement": <cql3Statement>,
        "values": [<valuesArray>]
    }
    
An example could look like

    {
        "action": "prepared",
        "statement": "INSERT INTO superkeyspace.tablewithinfos (id, key, value) VALUES(?, ? ,?)"
        "values": [
            ["3a708930-c005-11e3-8a33-0800200c9a66", "Key A", "Value A"],
            ["3a708931-c005-11e3-8a33-0800200c9a66", "Key B", "Value B"]
        ]
    }
    
#### Fields
`statement` A Cassandra Query Language version 3 (CQL3) compliant prepared statement query that is channeled through to the driver and Cassandra. Only *SELECT*, *UPDATE*, *INSERT* and *DELETE* are allowed.  
`statements` A JsonArray of Cassandra Query Language version 3 (CQL3) compliant prepared statement query that is channeled through to the driver and Cassandra. Only *SELECT*, *UPDATE*, *INSERT* and *DELETE* are allowed.
`values` A JsonArray of JsonArrays with the values. Every value list will create its bindings and be executed in a batched statement (if not a *SELECT* query).

#### Returns
*Note: Only for `SELECT`*
The `prepared` action returns a `JsonArray` of `JsonObject`s in the format `columnName:columnValue` (if any result is given) combining all results from all prepared invokes.

### Prepare

    {
        "action": "prepare",
        "statement": <cql3Statement> | "statements": [<cql3Statement>, ...]
    }
        
An example could look like

    {
        "action": "prepare",
        "statement": "INSERT INTO superkeyspace.tablewithinfos (id, value) VALUES(?, ?)"
    }

or

    {
        "action": "prepare",
        "statements": [
            "INSERT INTO superkeyspace.table (id, value) VALUES(?, ?)",
            "INSERT INTO superkeyspace.anothertable (id, value) VALUES(?, ?)"
        ]
    }
    
where `statement` is preferred over `statements`.

#### Fields
`statement` A Cassandra Query Language version 3 (CQL3) compliant query that is added to the prepared statement cache for later use.  
`statements` A JsonArray of Cassandra Query Language version 3 (CQL3) compliant queries, which are added to the prepared statement cache for later use.

#### Returns
Response as detailed in General Responses.

## Batching Operations
To reduce the amount of calls from a client user, it is possible to batch multiple actions and receive all their results in one response. Batching operations results from adding all JsonObject operations into a single JsonArray and sending this to a slightly different `address`.

	[
        {<OperationsJsonObject>}, ...
    ]

An example could look like

	[
		{
	        "action": "prepare",
	        "statement": "INSERT INTO superkeyspace.tablewithinfos (id, value) VALUES(?, ?)"
	    },
	    {
	        "action": "raw",
	        "statement": "SELECT * FROM superkeyspace.tablewithinfos"
	    }
	]

###Fields
`address` The address to send the batched operations to is the originally configured address plus `.batch`. Therefore, if the configuration is

	{
        "address": "your.awesome.persistency",
        "hosts": ["192.168.172.33", "192.168.172.34"],
        "port": 9142,
        "keyspace": "yourkeyspace"
    }

the address to send the JsonArray to would be `your.awesome.persistency.batch`.
*Note: If the original address ends on a `.`, the `.` won't be duplicated. If your address is `just.testing.` the resulting batch address would be `just.testing.batch`*

### Returns
The batched actions return their persistor operation corresponding result in one JsonArray.
*Note: Returned results are unordered*

### Note
The batched processing is another worker verticle invoked by the CassandraPersistor and splitting the input into its single actions, firing these agains the eventbus and cumulating the results for a single client response. Its calls are no direct invocation of the local CassandraPersistor verticle, but reuse the eventbus. Therefore, a batched call would also be balanced (if clustered).

## General Responses
In case no resultset is given to return to the sender or in case of errors a general status in JSON will be returned. It looks like

    {
        "status": "ok"
    }
    
or in case of errors

    {
        "status": "error",
        "message": <errorDescription>
    }

## Personal Note
*I don't know if this is very useful or already developed and published by others but I used it in private to test some ideas around Vert.x and Cassandra. As I was not able to find something similar very quickly I created this project. I hope this can be useful to you... with all its Bugs and Issues ;) If you like it you can give me a shout at [INsanityDesign][8].* 


  [1]: http://vertx.io
  [2]: http://cassandra.apache.org/
  [3]: http://www.datastax.com/documentation/developer/java-driver/2.0
  [4]: https://github.com/vert-x/mod-mongo-persistor
  [5]: https://github.com/nea/vertx-mod-cassandra-persistor/wiki/Version-History
  [6]: http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/Policies.html#defaultRetryPolicy()
  [7]: http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/policies/Policies.html#defaultReconnectionPolicy()
  [8]: http://www.insanitydesign.com/wp/