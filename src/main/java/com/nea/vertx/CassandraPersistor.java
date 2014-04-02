package com.nea.vertx;

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
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * 
 * @author nea@insanitydesign
 */
public class CassandraPersistor extends BusModBase implements Handler<Message<JsonObject>> {

	/** */
	private static Logger LOG;

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
		LOG = container.logger();

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
		LOG.info("[Cassandra Persistor] Connected to cluster: " + metadata.getClusterName() + "\n");
		//
		for(Host host : metadata.getAllHosts()) {
			LOG.info("[Cassandra Persistor] Datacenter: " + host.getDatacenter() + "; Host: " + host.getAddress() + "; Rack: "
					+ host.getRack() + "\n");
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
			// Create KEYSPACE, TABLE, INDEX
				case "create":
					create(message);
					break;

				// Insert
				case "insert":
					create(message);
					break;

				// Select
				case "select":
					select(message);
					break;

				// Channel the raw statements
				case "raw":
					select(message);
					break;

				// Batch every command, e.g. batch.insert
				case "batch":
					break;

				// Update
				case "update":
					break;

				// Delete
				case "delete":
					break;

				// Change KEYSPACE
				case "change":
					break;

				default:
					sendError(message, "[Cassandra Persistor] Action '" + action + "' unknown!");
			}
			//
		} catch(Exception e) {
			sendError(message, "[Cassandra Persistor] " + e.getMessage(), e);
		}
	}

	/**
	 * 
	 * @param message
	 */
	protected void select(Message<JsonObject> message) {
		//
		JsonObject selectMessage = message.body();
		
		//
		Select.Builder selection = QueryBuilder.select();

		// Select Fields
		JsonArray fields = selectMessage.getArray("fields");
		if(fields == null || fields.size() <= 0) {
			selection = QueryBuilder.select().all();
			
		} else {
			for(int i = fields.size(); i >= 0; --i) {
				((Selection)selection).column((String)fields.get(i));
			}	
		}
		
		// From Table
		Select select = selection.from(getKeyspace(), selectMessage.getString("table"));

		// Where
		String query = select.getQueryString();
		JsonArray whereStatements = selectMessage.getArray("where");
		if(whereStatements != null) {
			for(int i = whereStatements.size(); i >= 0; --i) {
				String whereStatement = whereStatements.get(i);
				query += " " + whereStatement;
				//
				if(i > 0) {
					query += " AND";
				}
			}	
		}
		
		//
		JsonArray retVals = new JsonArray();
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		//
		ResultSet resultSet = getSession().execute(query);
		int counter = 0;		
		//
		for(Row row : resultSet) {
			//
			JsonObject retVal = new JsonObject();
			//
			ColumnDefinitions rowColumnDefinitions = row.getColumnDefinitions();
			for(int i = 0; i < rowColumnDefinitions.size(); i++) {
				
				LOG.info(rowColumnDefinitions.getKeyspace(i));
				LOG.info(rowColumnDefinitions.getTable(i));
				LOG.info(rowColumnDefinitions.getName(i));				
				LOG.info(rowColumnDefinitions.getType(i));
				LOG.info(rowColumnDefinitions.getType(i).asJavaClass());
				LOG.info(rowColumnDefinitions.getType(i).getName());
				LOG.info(rowColumnDefinitions.getType(i).isCollection());
				LOG.info(rowColumnDefinitions.getType(i).getTypeArguments());
				LOG.info(DataType.set(DataType.varchar()));
				
				if(row.isNull(i)) {
					continue;
				}
				
				Object o = rowColumnDefinitions.getType(i).deserialize(row.getBytesUnsafe(i));
				JsonObject muh = new JsonObject();				
				System.out.println("Object: " + o);
				try {
//					retVals.add(ow.writeValueAsString(o));
					System.out.println(ow.writeValueAsString(o));
					retVal.putValue(rowColumnDefinitions.getName(i), ow.writeValueAsString(o).replaceAll("\"", ""));
					JsonArray maeh = new JsonArray();
					maeh.addString("muh");
					maeh.addNumber(1254);
					retVal.putElement("test", maeh);
					continue;
				} catch(JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(rowColumnDefinitions.getType(i).isCollection()) {
					continue;
				}
				
				
				
				//
				if(rowColumnDefinitions.getType(i) == DataType.uuid()) {
					retVal.putString(rowColumnDefinitions.getName(i), row.getUUID(i).toString());
					
				} else if(rowColumnDefinitions.getType(i) == DataType.varchar()) {
					retVal.putString(rowColumnDefinitions.getName(i), row.getString(i));
					
				} else if(rowColumnDefinitions.getType(i) == DataType.blob()) {
					retVal.putValue(rowColumnDefinitions.getName(i), row.getBytes(i));
					
				} else if(rowColumnDefinitions.getType(i).equals(DataType.set(DataType.varchar()))) {					
					retVal.putValue(rowColumnDefinitions.getName(i), row.getSet(i, rowColumnDefinitions.getType(i).getTypeArguments().get(0).asJavaClass()));
					
					
				} else {
					retVal.putValue(rowColumnDefinitions.getName(i), row.getBytes(i));
				}			
			}
			//
			retVals.addObject(retVal);			
		}
		
		//
		message.reply(retVals);
	}

	/**
	 * 
	 * @param message
	 */
	protected void create(Message<JsonObject> message) {

	}

	/**
	 * 
	 */
	@Override
	public void stop() {
		//
		if(getSession() != null) {
			getSession().close();
		}

		//
		if(getCluster() != null) {
			getCluster().close();
		}

		//
		eb.unregisterHandler(getAddress(), this);

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
