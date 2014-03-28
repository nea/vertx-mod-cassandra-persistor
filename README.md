# Vert.x Cassandra Persistor
This very simple [Vert.x][1] module allows to store and retrieve data from a [Cassandra][2] instance or cluster. It uses the the [DataStax Java Driver 2.0][3].

It is loosely based on the Vert.x [MongoDB persistor][4] and not optimized for highest performance but straight-forward integration with Cassandra. 

* Module `com.nea.vertx~mod-cassandra-persistor~X.X.X`
* Worker Verticle
* Multi-threaded

## Dependencies
This Persistor has only been developed and tested with Cassandra 2.x/CQL3. To use it you of course have to have a Cassandra instance running accesisible through the network, where this Module is running.

## Configuration
The Vert.x Cassandra Persistor takes the following configuration

    {
        "address": <address>,
        "hosts": [<hosts>],
        "keyspace": <keyspace>
    }

An exemplary configuration could look like

    {
        "address": "your.awesome.persistency",
        "hosts": ["192.168.172.33", "192.168.172.34"],
        "keyspace": "yourkeyspace"
    }

### Fields
* `address` The main address for the module. Every module has a main address. Defaults to `nea.vertx.cassandra.persistor`
* `hosts` A string array of host IPs the module connects to as contact points. Defaults to `127.0.0.1`
* `keyspace` The Cassandra keyspace to use. Defaults to `vertxpersistor`. If no keyspace is defined, `vertxpersistor` will try to create itself in Cassandra

## Operations

### Select
Query and return the data for the given arguments.

    {
        "action": "select",
        "fields": [<fieldName>],
        "table": <tableName>,
        "where": [<whereStatements>]
    }

An exemplary select could look like

    {
        "action": "select",
        "table": "testing",
        "where": ["id = 123456789"]
    }

and would result in the query

    SELECT * FROM yourkeyspace.testing WHERE id = 123456789;
    
#### Fields
* `fields` *optional* An array of <String> fields to query upon. If not given it will automatically `select().all()`
* `table` The table to query upon. The table will be expected under the configured `keyspace`
* `where` *optional* An array of <String> where conditions

### Insert
    {
        "action": "insert",
        "table": <tableName>,
        "fields": [<fieldName>],
        "values": [<values>]
    }

#### Fields

### Create
Create branches into multiple sub-actions, separated via `.` from the main action.

#### Keyspace
    {
        "action": "create.keyspace",
        "keyspace": <keyspaceName>,
        "replication": {<replicationStatement>}
    }

##### Fields

#### Table
    {
        "action": "create.table",
        "table": <tableName>,
        "fields": [<fieldName> <fieldType> <fieldAttribute>]
    }

##### Fields

#### Index
    {
        "action": "create.index",
        "table": <tableName>,
        "field": <fieldName>
    }

##### Fields

### Raw
*Please use with care!*

# Personal Note
*I don't know if this is very useful or already developed and published by others but I used it in private to test some ideas around Vert.x and Cassandra. As I was not able to find something similar to my idea I created this project. I hope this can be useful to you.* 

  [1]: http://vertx.io
  [2]: http://cassandra.apache.org/
  [3]: http://www.datastax.com/documentation/developer/java-driver/2.0
  [4]: https://github.com/vert-x/mod-mongo-persistor