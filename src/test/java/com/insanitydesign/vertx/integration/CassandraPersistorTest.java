package com.insanitydesign.vertx.integration;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertThat;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;

import java.nio.ByteBuffer;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * 
 * @author insanitydesign
 */
public class CassandraPersistorTest extends TestVerticle {

	/**
	 * Static block as @Before does not work with TestVerticle at the moment
	 */
	static {
		// Boot up Cassandra-Unit...
		try {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml");
			// ...and load the example data
			Session session = Cluster.builder().addContactPoint("127.0.0.1").build().connect();
			CQLDataLoader cqlDataLoader = new CQLDataLoader(session);
			cqlDataLoader.load(new ClassPathCQLDataSet("cassandraPersistorExampleData.cql"));

		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	@Override
	public void start() {
		// Start the "real" tests
		initialize();
		JsonObject config = new JsonObject();
		config.putArray("hosts", new JsonArray().add("127.0.0.1"));
		config.putString("compression", "SNAPPY");
		config.putString("retry", "fallthrough");
		config.putObject("reconnection", new JsonObject().putString("policy", "constant").putNumber("delay", 1000));
		config.putNumber("prepStmtCacheSize", 1);
		config.putNumber("batchTimeout", 10);

		//
		container.logger().info("[Cassandra Persistor Test] Starting test of module " + System.getProperty("vertx.modulename"));
		container.deployModule(System.getProperty("vertx.modulename"), config, 1, new AsyncResultHandler<String>() {
			public void handle(AsyncResult<String> asyncResult) {
				assertTrue(asyncResult.succeeded());
				assertNotNull("deploymentID should not be null", asyncResult.result());
				startTests();
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testWithResults() {
		//
		JsonObject select = new JsonObject();
		select.putString("action", "raw");
		select.putString("statement", "SELECT * FROM vertxpersistor.fulltable");

		//
		vertx.eventBus().send("vertx.cassandra.persistor", select, new Handler<Message<JsonArray>>() {
			@Override
			public void handle(Message<JsonArray> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertThat(reply.body(), instanceOf(JsonArray.class));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testWithoutResults() {
		//
		JsonObject select = new JsonObject();
		select.putString("action", "raw");
		select.putString("statement", "SELECT * FROM vertxpersistor.emptytable");

		//
		vertx.eventBus().send("vertx.cassandra.persistor", select, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("ok", reply.body().getString("status"));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testError() {
		//
		JsonObject select = new JsonObject();
		select.putString("action", "raw");
		select.putString("statement", "SELECT * FROM vertxpersistor.unavailabletable");

		//
		vertx.eventBus().send("vertx.cassandra.persistor", select, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("error", reply.body().getString("status"));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testCreateTable() {
		//
		JsonObject create = new JsonObject();
		create.putString("action", "raw");
		create.putString("statement", "CREATE TABLE vertxpersistor.newtable (id uuid PRIMARY KEY, field text)");

		//
		vertx.eventBus().send("vertx.cassandra.persistor", create, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("ok", reply.body().getString("status"));

					//
					JsonObject select = new JsonObject();
					select.putString("action", "raw");
					select.putString("statement", "SELECT * FROM vertxpersistor.newtable");

					//
					vertx.eventBus().send("vertx.cassandra.persistor", select, new Handler<Message<JsonObject>>() {
						@Override
						public void handle(Message<JsonObject> reply) {
							//
							try {
								container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

								// Tests
								assertNotNull(reply);
								assertNotNull(reply.body());

							} catch(Exception e) {
								e.printStackTrace();

							} finally {
								testComplete();
							}
						}
					});

				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testInsert() {
		//
		JsonObject insert = new JsonObject();
		insert.putString("action", "raw");
		String data = "Testing";
		String query = "INSERT INTO vertxpersistor.fulltable (id, key, value, data) VALUES(aaaaaaaa-2e54-4715-9f00-91dcbea6cf50, 'Unit', 'Test', 0x"
				+ DatatypeConverter.printHexBinary(data.getBytes()) + ")";
		insert.putString("statement", query);

		//
		vertx.eventBus().send("vertx.cassandra.persistor", insert, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("ok", reply.body().getString("status"));

					//
					JsonObject select = new JsonObject();
					select.putString("action", "raw");
					select.putString("statement", "SELECT * FROM vertxpersistor.fulltable WHERE id = aaaaaaaa-2e54-4715-9f00-91dcbea6cf50");

					//
					vertx.eventBus().send("vertx.cassandra.persistor", select, new Handler<Message<JsonArray>>() {
						@Override
						public void handle(Message<JsonArray> reply) {
							//
							try {
								container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

								// Tests
								assertNotNull(reply);
								assertNotNull(reply.body());
								assertEquals(1, reply.body().size());
								assertThat(reply.body().get(0), instanceOf(JsonObject.class));
								//
								JsonObject result = (JsonObject) reply.body().get(0);
								assertEquals("Unit", result.getString("key"));
								assertEquals("Test", result.getString("value"));
								ByteBuffer data = ByteBuffer.wrap(result.getBinary("data"));
								assertEquals("Testing", new String(data.array(), "UTF-8"));

							} catch(Exception e) {
								e.printStackTrace();

							} finally {
								testComplete();
							}
						}
					});

				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testBatchInsert() {
		//
		JsonObject insert = new JsonObject();
		insert.putString("action", "raw");
		JsonArray batch = new JsonArray();
		batch.addString("INSERT INTO vertxpersistor.fulltable (id, key, value) VALUES(aaaaaaaa-2e54-4715-9f00-91dcbea6cf50, 'Unit1', 'Test1')");
		batch.addString("INSERT INTO vertxpersistor.fulltable (id, key, value) VALUES(bbbbbbbb-2e54-4715-9f00-91dcbea6cf50, 'Unit2', 'Test2')");
		insert.putArray("statements", batch);

		//
		vertx.eventBus().send("vertx.cassandra.persistor", insert, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("ok", reply.body().getString("status"));

					//
					JsonObject select = new JsonObject();
					select.putString("action", "raw");
					select.putString("statement",
							"SELECT * FROM vertxpersistor.fulltable WHERE id IN(aaaaaaaa-2e54-4715-9f00-91dcbea6cf50, bbbbbbbb-2e54-4715-9f00-91dcbea6cf50)");

					//
					vertx.eventBus().send("vertx.cassandra.persistor", select, new Handler<Message<JsonArray>>() {
						@Override
						public void handle(Message<JsonArray> reply) {
							//
							container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

							// Tests
							assertNotNull(reply);
							assertNotNull(reply.body());
							assertEquals(2, reply.body().size());
							//
							for(Object result : reply.body()) {
								assertThat(result, instanceOf(JsonObject.class));
								//
								JsonObject resultRow = (JsonObject) result;
								assertTrue(resultRow.getString("key").startsWith("Unit"));
								assertTrue(resultRow.getString("value").startsWith("Test"));
							}

							//
							testComplete();
						}
					});

				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testPreparedInsert() {
		//
		Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		//
		JsonObject insert = new JsonObject();
		insert.putString("action", "prepared");
		insert.putString("statement", "INSERT INTO vertxpersistor.fulltable (id, key, value, number, date) VALUES(?, ?, ?, ?, ?)");
		//
		JsonArray values1 = new JsonArray();
		values1.addString("aaaaaaaa-2e54-4715-9f00-91dcbea6cf50");
		values1.addString("Unit1");
		values1.addString("Test1");
		values1.addNumber(2014);
		values1.addString(format.format(new Date()));
		//
		JsonArray values2 = new JsonArray();
		values2.addString("bbbbbbbb-2e54-4715-9f00-91dcbea6cf50");
		values2.addString("Unit2");
		values2.addString("Test2");
		values2.addNumber(2015);
		values2.addString(format.format(new Date()));
		//
		JsonArray values = new JsonArray();
		values.addArray(values1);
		values.addArray(values2);
		//
		insert.putArray("values", values);

		//
		vertx.eventBus().send("vertx.cassandra.persistor", insert, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("ok", reply.body().getString("status"));

					//
					JsonObject select = new JsonObject();
					select.putString("action", "raw");
					select.putString("statement",
							"SELECT * FROM vertxpersistor.fulltable WHERE id IN(aaaaaaaa-2e54-4715-9f00-91dcbea6cf50, bbbbbbbb-2e54-4715-9f00-91dcbea6cf50)");

					//
					vertx.eventBus().send("vertx.cassandra.persistor", select, new Handler<Message<JsonArray>>() {
						@Override
						public void handle(Message<JsonArray> reply) {
							//
							container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

							// Tests
							assertNotNull(reply);
							assertNotNull(reply.body());
							assertEquals(2, reply.body().size());
							//
							for(Object result : reply.body()) {
								assertThat(result, instanceOf(JsonObject.class));
								//
								JsonObject resultRow = (JsonObject) result;
								assertTrue(resultRow.getString("key").startsWith("Unit"));
								assertTrue(resultRow.getString("value").startsWith("Test"));
							}

							testComplete();
						}
					});

				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testPreparedSelect() {
		//
		JsonObject insert = new JsonObject();
		insert.putString("action", "prepared");
		insert.putString("statement", "SELECT * FROM vertxpersistor.fulltable WHERE id = ?");
		//
		JsonArray values1 = new JsonArray();
		values1.addString("756716f7-2e54-4715-9f00-91dcbea6cf50");
		JsonArray values2 = new JsonArray();
		values2.addString("156716f7-2e54-4715-9f00-91aaaea6cf50");
		JsonArray values = new JsonArray();
		values.addArray(values1);
		values.addArray(values2);
		//
		insert.putArray("values", values);

		//
		vertx.eventBus().send("vertx.cassandra.persistor", insert, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertThat(reply.body(), instanceOf(JsonArray.class));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}

	/**
	 * 
	 */
	@Test
	public void testPrepareSingleStatement() {
		JsonObject prepare = new JsonObject();
		prepare.putString("action", "prepare");
		prepare.putString("statement", "SELECT * FROM vertxpersistor.fulltable WHERE id = ?");

		vertx.eventBus().send("vertx.cassandra.persistor", prepare, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("ok", reply.body().getString("status"));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}

	/**
     * 
     */
	@Test
	public void testPrepareMultipleStatements() {
		JsonObject prepare = new JsonObject();
		prepare.putString("action", "prepare");
		JsonArray statements = new JsonArray();
		statements.addString("SELECT * FROM vertxpersistor.emptytable WHERE id = ?");
		statements.addString("SELECT * FROM vertxpersistor.fulltable WHERE id = ?");
		prepare.putArray("statements", statements);

		vertx.eventBus().send("vertx.cassandra.persistor", prepare, new Handler<Message<JsonObject>>() {
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertEquals("ok", reply.body().getString("status"));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}
	
	
	/**
	 * 
	 */
	@Test
	public void testBatchActions() {
		//
		JsonArray batchedActions = new JsonArray();

		//
		JsonObject statement1 = new JsonObject();
		statement1.putString("action", "prepare");
		statement1.putString("statement", "INSERT INTO vertxpersistor.fulltable (id, date, isValid) VALUES(?, ?, ?)");
		//
		JsonObject statement2 = new JsonObject();
		statement2.putString("action", "raw");
		statement2.putString("statement", "SELECT * FROM vertxpersistor.fulltable WHERE id = 156716f7-2e54-4715-9f00-91aaaea6cf50");
		//
		batchedActions.add(statement1);
		batchedActions.add(statement2);

		//
		vertx.eventBus().send("vertx.cassandra.persistor.batch", batchedActions, new Handler<Message<JsonObject>>() {
			/**
			 * 
			 */
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertThat(reply.body(), instanceOf(JsonArray.class));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}
	
	
	/**
	 * 
	 */
	@Test
	public void testBrokenBatchAction() {
		//
		JsonArray batchedActions = new JsonArray();

		//
		JsonObject statement1 = new JsonObject();
		statement1.putString("action", "broken");
		statement1.putString("statement", "INSERT INTO vertxpersistor.fulltable (id, date, isValid) VALUES(?, ?, ?)");
		//
		JsonObject statement2 = new JsonObject();
		statement2.putString("action", "raw");
		statement2.putString("statement", "SELECT * FROM vertxpersistor.fulltable WHERE id = 156716f7-2e54-4715-9f00-91aaaea6cf50");
		//
		batchedActions.add(statement1);
		batchedActions.add(statement2);

		//
		vertx.eventBus().send("vertx.cassandra.persistor.batch", batchedActions, new Handler<Message<JsonObject>>() {
			/**
			 * 
			 */
			@Override
			public void handle(Message<JsonObject> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());

					// Tests
					assertNotNull(reply);
					assertNotNull(reply.body());
					assertThat(reply.body(), instanceOf(JsonArray.class));

				} catch(Exception e) {
					e.printStackTrace();

				} finally {
					testComplete();
				}
			}
		});
	}
	
	/**
	 * 
	 */
	@Test
	public void testMultiProcesses() {
		//
		final int testAmount = 50;
		
		//
		Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		//
		final JsonObject insert = new JsonObject();
		insert.putString("action", "prepared");
		insert.putString("statement", "INSERT INTO vertxpersistor.fulltable (id, key, value, number, date) VALUES(?, ?, ?, ?, ?)");
		//
		JsonArray values1 = new JsonArray();
		values1.addString("cefcefce-2e54-4715-9f00-91dcbea6cf50");
		values1.addString("Unit1");
		values1.addString("Test1");
		values1.addNumber(2014);
		values1.addString(format.format(new Date()));
		//
		JsonArray values = new JsonArray();
		values.addArray(values1);
		//
		insert.putArray("values", values);
		
		//
		ExecutorService executor = Executors.newFixedThreadPool(10);
		for(int i = 0; i < testAmount; i++) {
			executor.submit(new Runnable() {			
				@Override
				public void run() {
					vertx.eventBus().send("vertx.cassandra.persistor", insert, new Handler<Message<JsonObject>>() {
						@Override
						public void handle(Message<JsonObject> reply) {
							System.out.println("[" + getClass().getName() + "] Reply Body: " + reply.body() + " @" + new Date());
							
							try {
								// Basic Tests
								assertNotNull(reply);
								assertNotNull(reply.body());
								assertEquals("ok", reply.body().getString("status"));

							} catch(Exception e) {
								e.printStackTrace();
							}
						}
					});
				}
			});
		}
		
		//
		try {
			//Close down all submitted threads
			executor.shutdown();
			//but wait a maximum of 30 seconds before terminating the test
			executor.awaitTermination(30000, TimeUnit.SECONDS);
			
		} catch(InterruptedException e) {
			e.printStackTrace();
			
		} finally {
			testComplete();
		}
	}
}
