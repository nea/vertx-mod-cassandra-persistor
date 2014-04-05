CREATE KEYSPACE vertxpersistor WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};
USE vertxpersistor;

CREATE TABLE emptytable(
    id uuid PRIMARY KEY,
    key varchar,
    value varchar);

CREATE TABLE fulltable(
    id uuid PRIMARY KEY,
    key varchar,
    value varchar);

INSERT INTO fulltable (id, key, value) VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 1', 'Hello');
INSERT INTO fulltable (id, key, value) VALUES (756716f7-2e54-4715-9f00-91dcb3a6cf50, 'Key 2', 'World');
INSERT INTO fulltable (id, key, value) VALUES (756716f7-2e54-4715-9f00-91dcfea6cf50, 'Key 3', 'You');
INSERT INTO fulltable (id, key, value) VALUES (756716f7-2e54-4715-9f00-91dcb1a6cf50, 'Key 4', 'Are');
INSERT INTO fulltable (id, key, value) VALUES (756716f7-2e54-4715-9f00-91dcb3a6cf50, 'Key 5', 'Using');
INSERT INTO fulltable (id, key, value) VALUES (756716f7-2e54-4715-9f00-91d2bea6cf50, 'Key 6', 'The');
INSERT INTO fulltable (id, key, value) VALUES (856716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 7', 'Vert.x');
INSERT INTO fulltable (id, key, value) VALUES (956716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 8', 'Cassandra');
INSERT INTO fulltable (id, key, value) VALUES (156716f7-2e54-4715-9f00-91dcbea6cf50, 'Key 9', 'Persistor');