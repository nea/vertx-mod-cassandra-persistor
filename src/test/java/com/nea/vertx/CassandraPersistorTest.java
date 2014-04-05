package com.nea.vertx;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertThat;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;

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
 * @author nea@insanitydesign
 */
public class CassandraPersistorTest extends TestVerticle {

	/**
	 * 
	 */
	@Override
	public void start() {
		//Boot up Cassandra-Unit...
		try {			
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
			//...and load the example data
			Session session = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build().connect();
			CQLDataLoader cqlDataLoader = new CQLDataLoader(session);
			cqlDataLoader.load(new ClassPathCQLDataSet("cassandraPersistorExampleData.cql"));
			
		} catch(Exception e) {
			e.printStackTrace();
			return;
		}			
		
		//Start the "real" tests
		initialize();
		JsonObject config = new JsonObject();
		config.putArray("hosts", new JsonArray().add("127.0.0.1"));
		config.putNumber("port", 9142);

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
	public void testRawStatements() {
		//
		container.logger().info("[" + getClass().getName() + "] Testing raw Statements...");

		//
		JsonObject select = new JsonObject();
		select.putString("action", "raw");
		select.putString("statement", "SELECT * FROM vertxpersistor.fulltable");

		//
		vertx.eventBus().send("nea.vertx.cassandra.persistor", select, new Handler<Message<JsonArray>>() {
			@Override
			public void handle(Message<JsonArray> reply) {
				//
				try {
					container.logger().info("[" + getClass().getName() + "] Reply Body: " + reply.body());
					
					//Tests
					assertNotNull(reply);
					assertNotNull(reply.body());					
					assertThat(reply.body(), instanceOf(JsonArray.class));
					
					//
					container.logger().info("[" + getClass().getName() + "] ...tested raw statements!");
					
				} catch(Exception e) {					
				}
				
				//
				testComplete();
			}
		});
	}

	/**
	 * 
	 */
	@Override
	public void stop() {
		//
		super.stop();
		//Clean up the embedded server again
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}
}
