package com.nea.vertx;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

/**
 * 
 * @author savas-simon.ziplies
 */
public class CassandraPersistorTest extends TestVerticle {

	/**
	 * 
	 */
	@Override
	public void start() {
		initialize();

		//
		JsonObject config = new JsonObject();
		config.putArray("hosts", new JsonArray().add("10.10.10.10"));

		//
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
		container.logger().info("[Cassandra Persistor] Testing raw Statements...");

		//
		JsonObject select = new JsonObject();
		select.putString("action", "raw");
		select.putString("statement", "SELECT * FROM songs");

		//
		vertx.eventBus().send("nea.vertx.cassandra.persistor", select, new Handler<Message<JsonArray>>() {
			@Override
			public void handle(Message<JsonArray> reply) {
				//
				assertNotNull(reply);
				
				for(Object o : reply.body()) {
					System.out.println("Oooo" + o);
					System.out.println(o instanceof JsonObject);
					System.out.println(((JsonObject) o).getString("artist"));
				}
				
				assertNotNull(reply.body());
				
				//
				container.logger().info("...tested raw statements!");
				testComplete();
			}
		});
	}
}
