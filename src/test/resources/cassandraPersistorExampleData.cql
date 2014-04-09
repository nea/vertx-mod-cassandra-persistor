CREATE KEYSPACE vertxpersistor WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};
USE vertxpersistor;

CREATE TABLE emptytable(
    id uuid PRIMARY KEY,
    key varchar,
    value varchar);

CREATE TABLE fulltable(
    id uuid PRIMARY KEY,
    key varchar,
    value varchar,
    valueset set<text>,
    valuelist list<varchar>,
    valuemap map<varchar, varchar>,
    date timestamp,
    isValid boolean,
    number int
    );

INSERT INTO fulltable (id, key, value, valueset) VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 1', 'Hello', {'Set A', 'Set B'});
INSERT INTO fulltable (id, key, value, valueset) VALUES (756716f7-2e54-4715-9f00-91dcb3a6cf50, 'Key 2', 'World!', {'Set A', 'Set B'});
INSERT INTO fulltable (id, key, value, valueset) VALUES (756716f7-2e54-4715-9f00-91dcfea6cf50, 'Key 3', 'You', {'Set A', 'Set B'});
INSERT INTO fulltable (id, key, value, valueset) VALUES (756716f7-2e54-4715-9f00-91dcb1a6cf50, 'Key 4', 'Are', {'Set A', 'Set B'});
INSERT INTO fulltable (id, key, value, valueset) VALUES (756716f7-2e54-4715-9f00-91dcb3a6cf50, 'Key 5', 'Using', {'Set A', 'Set B'});

INSERT INTO fulltable (id, key, value, valueset, valuelist) VALUES (756716f7-2e54-4715-9f00-91d2bea6cf50, 'Key 6', 'The', {'Set A', 'Set B'}, ['List A', 'List B']);
INSERT INTO fulltable (id, key, value, valueset, valuelist) VALUES (856716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 7', 'Vert.x', {'Set A', 'Set B'}, ['List A', 'List B']);
INSERT INTO fulltable (id, key, value, valueset, valuelist) VALUES (956716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 8', 'Cassandra', {'Set A', 'Set B'}, ['List A', 'List B']);
INSERT INTO fulltable (id, key, value, valueset, valuelist) VALUES (156716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 9', 'Persistor', {'Set A', 'Set B'}, ['List A', 'List B']);

INSERT INTO fulltable (id, valueset, valuelist, valuemap) VALUES (156716f7-2e54-4715-9f00-91aaaea6cf50, {'Set A', 'Set B'}, ['List A', 'List B'], {'Map A': 'Map AA', 'Map B': 'Map BB'});

INSERT INTO fulltable (id, date, isValid) VALUES (756716f7-2e54-4715-9f00-91abc3a6cf50, '2012-10-20 12:10', true);
INSERT INTO fulltable (id, date, isValid) VALUES (756716f7-2e54-4715-9f00-91def3a6cf50, 86400, false);