# Vert.x Cassandra Persistor [![Build Status](https://travis-ci.org/nea/vertx-mod-cassandra-persistor.svg?branch=master)](https://travis-ci.org/nea/vertx-mod-cassandra-persistor)
This very simple [Vert.x][1] module allows to store and retrieve data from a [Cassandra][2] instance or cluster. It uses the the [DataStax Java Driver 2.0][3].

It is loosely based on the Vert.x [MongoDB persistor][4] and not optimized for highest performance (e.g. also no prepared statements) but straight-forward integration with Cassandra. 

* Module `com.insanitydesign~vertx-mod-cassandra-persistor~X.X.X`
* Worker Verticle
* EventBus JSON-based
* CQL3

## Latest Versions
* 0.1.1
    * Changed group-id and default module address
* 0.1.0
    * Added `raw` batch support 
* 0.0.3
    * Added support for basic `map`s
    * Added support for `date` type

For a full version history go [here][5].

## Dependencies
This Persistor has only been developed and tested with Cassandra 2.x/CQL3. To use it you of course have to have a Cassandra instance running and accessible through the network, where this Module is running.

## Configuration
The Vert.x Cassandra Persistor takes the following configuration

    {
        "address": <address>,
        "hosts": [<hosts>],
        "port": <port>,
        "keyspace": <keyspace>
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

## Operations

### Raw
*Please use with care!*

    {
        "action": "raw",
        "statement": <cql3Statement> | "statements": [<cql3BatchStatements>]
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
The `raw` action returns a `JsonArray` of `JsonObject`s in the format `columnName:columnValue` (if any result is given). *Note: Value types are not fully interpreted but generally covered as numbers, strings or collections. Complex Types are not handled at the moment!*

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
*I don't know if this is very useful or already developed and published by others but I used it in private to test some ideas around Vert.x and Cassandra. As I was not able to find something similar very quickly I created this project. I hope this can be useful to you... with all its Bugs and Issues ;)* 


  [1]: http://vertx.io
  [2]: http://cassandra.apache.org/
  [3]: http://www.datastax.com/documentation/developer/java-driver/2.0
  [4]: https://github.com/vert-x/mod-mongo-persistor
  [5]: https://github.com/nea/vertx-mod-cassandra-persistor/wiki/Version-History