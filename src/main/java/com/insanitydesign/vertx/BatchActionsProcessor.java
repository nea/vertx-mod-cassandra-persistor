package com.insanitydesign.vertx;

import java.util.concurrent.atomic.AtomicInteger;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * 
 * @author insanitydesign
 */
public class BatchActionsProcessor extends BusModBase implements Handler<Message<JsonArray>> {

	/** The configured address or default */
	private String address;

	/** The configured CassandraPersistor address or default */
	private String persistorAddress;

	/** The timeout after which the batch reply should be returned anyways */
	private long timeout = 30000;

	/**
	 * 
	 */
	@Override
	public void start() {
		//
		super.start();
		//
		logger.info("[BatchActions Processor] Booting up...");

		//
		setAddress(getOptionalStringConfig("address", "vertx.cassandra.persistor"));
		setTimeout(getOptionalLongConfig("batchTimeout", 30000));
		//
		eb.registerHandler(getAddress(), this);

		//
		logger.info("[BatchActions Processor] ...booted!");
	}

	/**
	 * 
	 */
	@Override
	public void handle(Message<JsonArray> message) {
		//
		JsonArray actions = message.body();
		BatchRequestHandler<?> batchRequestHandler = new BatchRequestHandler<>(message, actions.size());
		//
		for(int i = 0; i < actions.size(); i++) {
			eb.sendWithTimeout(getPersistorAddress(), (JsonObject) actions.get(i), getTimeout(), batchRequestHandler);
		}
	}

	/* ***** GETTER/SETTER ***** */
	/**
	 * 
	 * @return
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * 
	 * @param address
	 */
	public void setAddress(String address) {
		//
		this.persistorAddress = address;
		//
		if(address.charAt(address.length() - 1) == '.') {
			address = address.substring(0, address.length() - 1);
		}
		this.address = address + ".batch";
	}

	/**
	 * 
	 * @return
	 */
	public String getPersistorAddress() {
		return persistorAddress;
	}

	/**
	 * 
	 * @param timeout
	 */
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	/**
	 * 
	 * @return
	 */
	public long getTimeout() {
		return timeout;
	}

	/**
	 * 
	 * 
	 */
	public static class BatchRequestHandler<T> implements Handler<AsyncResult<Message<T>>> {

		/** */
		private Message<JsonArray> message;

		/** */
		private AtomicInteger actionsAmount;

		/** */
		private JsonArray retVals;

		/**
		 * 
		 * @param message
		 * @param actionsAmount
		 */
		public BatchRequestHandler(Message<JsonArray> message, int actionsAmount) {
			this.message = message;
			this.actionsAmount = new AtomicInteger(actionsAmount);
			this.retVals = new JsonArray();
		}

		/**
		 * 
		 */
		@Override
		public void handle(AsyncResult<Message<T>> result) {
			if(result.succeeded()) {
				// Add to global return object
				this.retVals.add(result.result().body());
			}

			// Reply when all has arrived or if an issue popped up decrement and check
			if(this.actionsAmount.decrementAndGet() <= 0) {
				//If no results where received, something most likely went wrong
				if(this.retVals.toList().isEmpty()) {
					this.retVals.add(new JsonObject().putString("status", "error").putString("message", "[BatchRequest Handler] Received no results!"));
				}
				message.reply(this.retVals);
			}
		}

		/* ***** GETTER/SETTER ***** */
		/**
		 * 
		 * @return
		 */
		public AtomicInteger getActionsAmount() {
			return actionsAmount;
		}

		/**
		 * 
		 * @return
		 */
		public Message<JsonArray> getMessage() {
			return message;
		}
	}
}
