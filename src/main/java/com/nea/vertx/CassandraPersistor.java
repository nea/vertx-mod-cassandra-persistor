package com.nea.vertx;

import java.util.Collection;
import java.util.HashSet;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * 
 * @author nea@insanitydesign
 */
public class CassandraPersistor extends BusModBase implements Handler<Message<JsonObject>> {

	/** */
	private Logger LOG;

	/** */
	private Cluster cluster;

	/** */
	private Session session;

	/* Configuration */
	/** */
	private String address;
	/** */
	private JsonArray hosts;
	/** */
	private String keyspace;

	/**
	 * 
	 */
	@Override
	public void start() {
		//
		super.start();

		//
		this.LOG = container.logger();

		//
		setAddress(getOptionalStringConfig("address", "nea.vertx.cassandra.persistor"));
		setHosts(getOptionalArrayConfig("hosts", new JsonArray("[\"127.0.0.1\"]")));
		setKeyspace(getOptionalStringConfig("keyspace", "vertxpersistor"));

		//
		Cluster.Builder builder = Cluster.builder();
		for(int i = this.hosts.size() - 1; i >= 0; --i) {
			builder.addContactPoint((String) this.hosts.get(i));
		}
		setCluster(builder.build());

		//
		Metadata metadata = this.cluster.getMetadata();
		LOG.info("[Cassandra Persistor] Connected to cluster: " + metadata.getClusterName());
		//
		for(Host host : metadata.getAllHosts()) {
			LOG.info("[Cassandra Persistor] DC: " + host.getDatacenter() + " - Host: " + host.getAddress() + " - Rack: " + host.getRack());
		}

		//
		try {
			setSession(getCluster().connect());

		} catch(Exception e) {
			sendError(null, "[Cassandra Persistor] Cannot connect/get session from Cassandra!");
		}

		//
		eb.registerHandler(getAddress(), this);
	}

	/**
	 * 
	 */
	@Override
	public void handle(Message<JsonObject> message) {
		//
		String action = message.body().getString("action");

		//
		if(action == null) {
			sendError(message, "[Cassandra Persistor] Please specify an action!");
			return;
		}

		//
		try {
			switch(action) {
			// Channel the raw statements
				case "raw":
					raw(message);
					break;

				default:
					sendError(message, "[Cassandra Persistor] Action '" + action + "' unknown!");
			}

			//
		} catch(Exception e) {
			sendError(message, e);
		}
	}
	
	
	

	/**
	 * 
	 * @param message
	 */
	protected void raw(Message<JsonObject> message) {
		//
		JsonObject rawMessage = message.body();
		String query = rawMessage.getString("statement");

		//
		ResultSet resultSet = null;
		try {
			resultSet = getSession().execute(query);

		} catch(Exception e) {
			// An error happened
			sendError(message, e);
			return;
		}

		// Query went through but without results
		if(resultSet == null || resultSet.getAvailableWithoutFetching() <= 0) {
			sendOK(message);
			return;
		}
		
		//The returned result array
		JsonArray retVals = new JsonArray();

		// Iterate the results
		for(Row row : resultSet) {
			//Row result
			JsonObject retVal = new JsonObject();
			
			// Get the column definitions to iterate over the different types and check
			ColumnDefinitions rowColumnDefinitions = row.getColumnDefinitions();
			for(int i = 0; i < rowColumnDefinitions.size(); i++) {
				// Null empty columns
				if(row.isNull(i)) {
					continue;
				}
				
				//Read the column bytes unsafe and operate on the deserialized object instead of iterating over the type of the definitions
				Object columnValue = rowColumnDefinitions.getType(i).deserialize(row.getBytesUnsafe(i));
				
				//Parse the returning object to a supported type
				retVal = addColumn(rowColumnDefinitions.getName(i), columnValue, retVal);				
			}
			
			//Add the row
			retVals.addObject(retVal);
		}
		
		//Return the result array
		message.reply(retVals);

		//
//		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
//		int counter = 0;
//		//
//		for(Row row : resultSet) {
//			//
//			JsonObject retVal = new JsonObject();
//			//
//			ColumnDefinitions rowColumnDefinitions = row.getColumnDefinitions();
//			for(int i = 0; i < rowColumnDefinitions.size(); i++) {
//
//				LOG.info(rowColumnDefinitions.getKeyspace(i));
//				LOG.info(rowColumnDefinitions.getTable(i));
//				LOG.info(rowColumnDefinitions.getName(i));
//				LOG.info(rowColumnDefinitions.getType(i));
//				LOG.info(rowColumnDefinitions.getType(i).asJavaClass());
//				LOG.info(rowColumnDefinitions.getType(i).getName());
//				LOG.info(rowColumnDefinitions.getType(i).isCollection());
//				LOG.info(rowColumnDefinitions.getType(i).getTypeArguments());
//				LOG.info(DataType.set(DataType.varchar()));
//
//				if(row.isNull(i)) {
//					continue;
//				}
//
//				Object o = rowColumnDefinitions.getType(i).deserialize(row.getBytesUnsafe(i));
//				JsonObject muh = new JsonObject();
//				System.out.println("Object: " + o);
//				try {
//					// retVals.add(ow.writeValueAsString(o));
//					System.out.println(ow.writeValueAsString(o));
//					retVal.putValue(rowColumnDefinitions.getName(i), ow.writeValueAsString(o).replaceAll("\"", ""));
//					JsonArray maeh = new JsonArray();
//					maeh.addString("muh");
//					maeh.addNumber(1254);
//					retVal.putElement("test", maeh);
//					continue;
//				} catch(JsonProcessingException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//
//				if(rowColumnDefinitions.getType(i).isCollection()) {
//					continue;
//				}
//
//				//
//				if(rowColumnDefinitions.getType(i) == DataType.uuid()) {
//					retVal.putString(rowColumnDefinitions.getName(i), row.getUUID(i).toString());
//
//				} else if(rowColumnDefinitions.getType(i) == DataType.varchar()) {
//					retVal.putString(rowColumnDefinitions.getName(i), row.getString(i));
//
//				} else if(rowColumnDefinitions.getType(i) == DataType.blob()) {
//					retVal.putValue(rowColumnDefinitions.getName(i), row.getBytes(i));
//
//				} else if(rowColumnDefinitions.getType(i).equals(DataType.set(DataType.varchar()))) {
//					retVal.putValue(rowColumnDefinitions.getName(i),
//							row.getSet(i, rowColumnDefinitions.getType(i).getTypeArguments().get(0).asJavaClass()));
//
//				} else {
//					retVal.putValue(rowColumnDefinitions.getName(i), row.getBytes(i));
//				}
//			}
//			//
//			retVals.addObject(retVal);
//		}
//
//		//
//		message.reply(retVals);
	}
	
	/**
	 * 
	 * @param columnName
	 * @param columnValue
	 * @param retVal
	 * @return
	 */
	protected JsonObject addColumn(String columnName, Object columnValue, JsonObject retVal) {
		//
		if(columnValue instanceof Number) {
			retVal.putNumber(columnName, (Number)columnValue);
			
		} else if(columnValue instanceof String) {
			retVal.putString(columnName, (String)columnValue);
			
		} else if(columnValue instanceof Collection<?>) {
			//
			JsonArray retArray = new JsonArray();
			//
			retArray = addColumnCollection((Collection<?>)columnValue, retArray);
			//
			retVal.putArray(columnName, retArray);
		}
		
		//
		return retVal;
	}
	
	/**
	 * 
	 * @param columnValue
	 * @param retVal
	 * @return
	 */
	protected JsonArray addColumnCollection(Collection<?>columnValue, JsonArray retVal) {
		//
		for(Object collectionValue : columnValue) {
			//
			if(collectionValue instanceof Number) {
				retVal.addNumber((Number)collectionValue);
				
			} else if(collectionValue instanceof String) {
				retVal.addString((String)collectionValue);
				
			} else if(collectionValue instanceof Collection<?>) {
				retVal.addArray(addColumnCollection((Collection<?>)collectionValue, new JsonArray()));
			}
		}
		
		//
		return retVal;
	}

	/**
	 * 
	 * @param message
	 * @param e
	 */
	protected void sendError(Message<JsonObject> message, Exception e) {
		sendError(message, "[Cassandra Persistor] " + e.getMessage(), e);
	}

	/**
	 * 
	 */
	@Override
	public void stop() {
		//
		eb.unregisterHandler(getAddress(), this);

		//
		if(getSession() != null) {
			getSession().close();
		}

		//
		if(getCluster() != null) {
			getCluster().close();
		}

		//
		super.stop();
	}

	/* ***** GETTER/SETTER ***** */
	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public JsonArray getHosts() {
		return hosts;
	}

	public void setHosts(JsonArray hosts) {
		this.hosts = hosts;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
}
