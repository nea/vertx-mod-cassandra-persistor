package com.nea.vertx;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

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
		setSession(getCluster().connect());
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
				//Batch every command, e.g. batch.insert
				case "batch":
					break;
				
				// Create KEYSPACE, TABLE, INDEX
				case "create":
					create(message);
					break;

				//Insert
				case "insert":
					create(message);
					break;

				//Select
				case "select":
					read(message);
					break;
					
				//Update
				case "update":
					read(message);
					break;
					
				//Delete
				case "delete":
					read(message);
					break;
					
				//Change KEYSPACE
				case "change":
					read(message);
					break;
					
				//Channel through raw statements
				case "raw":
					read(message);
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
	protected void create(Message<JsonObject> message) {
		
	}
	
	
	/**
	 * 
	 * @param message
	 */
	protected void read(Message<JsonObject> message) {
		Select query = QueryBuilder
			      .select()
			      .all()
			      .from(getKeyspace(), "");
		
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
