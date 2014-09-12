package com.insanitydesign.vertx;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.vertx.java.core.logging.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * This serves as cache for prepared statements, implementing logic to handle new, existing and also limiting cache
 * stack sizes.
 * 
 * @author insanitydesign
 */
public class PreparedStatementCache {

	/** A concurrency safe HashMap to store all cached statements */
	private Map<String, CassandraPreparedStatement> cachedStatements = new ConcurrentHashMap<>();

	/** The max size of this cache before statements are removed */
	private int prepStmtCacheSize;

	/** The Cassandra storage connection to create prepared statements from */
	private Session session;

	/** The container logger for debugging purposes */
	private Logger logger;

	/**
	 * Initiate the cache with its max size and a logger to debug against.
	 * 
	 * @param prepStmtCacheSize
	 *            The size of this cache
	 * @param logger
	 *            The logger to use for debugging
	 */
	public PreparedStatementCache(int prepStmtCacheSize, Logger logger) {
		this.prepStmtCacheSize = prepStmtCacheSize;
		this.logger = logger;
	}

	/**
	 * Checks if a statement is already cached and returns that while also incrementing its counter. If the statement is
	 * not part of the cache, it will be added upfront.
	 * 
	 * @param statement
	 *            The CQL Statement to prepare and cache
	 * @return A PreparedStatement to fire
	 */
	public PreparedStatement addAndGet(String statement) {
		if(!contains(statement)) {
			return add(statement).useAndGet();
		}

		return get(statement).useAndGet();
	}

	/**
	 * Add the given statement by checking if the cache size has been reached. If not, just add it to the containers
	 * cache. If reached, check for the least used statement, remove that and add the given statement as new
	 * PreparedStatement
	 * 
	 * @param statement
	 *            The CQL Statement to prepare and cache
	 * @return An internal CassandraPreparedStatement containing the PreparedStament and counter
	 */
	public CassandraPreparedStatement add(String statement) {
		//
		if(size() >= getPrepStmtCacheSize()) {
			//
			String statementToRemove = "";
			int minUsage = Integer.MAX_VALUE;
			// Find least used and remove
			for(Entry<String, CassandraPreparedStatement> entry : this.cachedStatements.entrySet()) {
				if(entry.getValue().getUsage() < minUsage) {
					minUsage = entry.getValue().getUsage();
					statementToRemove = entry.getKey();
				}
			}
			//
			if(logger.isDebugEnabled()) {
				logger.debug("[Cassandra Persistor] Removing " + statementToRemove + " from prepared statement cache!");
			}
			remove(statementToRemove);
		}
		//
		put(statement, getSession().prepare(statement));
		return get(statement);
	}

	/**
	 * Iterates and adds a list of given statements through @see add(String statement)
	 * 
	 * @param statements
	 */
	public void addAll(List<String> statements) {
		for(String statement : statements) {
			add(statement);
		}
	}

	/**
	 * The only method to add a prepared statement to the cache by simply putting it.
	 * 
	 * @param statement
	 * @param preparedStatement
	 * @return The previously associated CassandraPreparedStatement or null if none
	 */
	public CassandraPreparedStatement put(String statement, PreparedStatement preparedStatement) {
		//
		if(logger.isDebugEnabled()) {
			logger.debug("[Cassandra Persistor] Adding " + statement + " to prepared statement cache!");
		}
		return this.cachedStatements.put(statement, new CassandraPreparedStatement(preparedStatement));
	}

	/**
	 * The only method to remove a prepared statement by simply removing it.
	 * 
	 * @param statement
	 * @return The CassandraPreparedStatement of the assigned key or null
	 */
	public CassandraPreparedStatement remove(String statement) {
		return this.cachedStatements.remove(statement);
	}

	/**
	 * Return the size of the backing map.
	 * 
	 * @return
	 */
	public int size() {
		return this.cachedStatements.size();
	}

	/**
	 * 
	 * @param statement
	 * @return
	 */
	protected boolean contains(String statement) {
		return this.cachedStatements.containsKey(statement);
	}

	/**
	 * 
	 * @param statement
	 * @return
	 */
	protected CassandraPreparedStatement get(String statement) {
		return this.cachedStatements.get(statement);
	}

	/* ***** GETTER/SETTER ***** */
	/**
	 * 
	 * @return
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * 
	 * @param session
	 */
	public void setSession(Session session) {
		this.session = session;
	}

	/**
	 * 
	 * @return
	 */
	public int getPrepStmtCacheSize() {
		return prepStmtCacheSize;
	}

	/**
	 * 
	 * @param prepStmtCacheSize
	 */
	public void setPrepStmtCacheSize(int prepStmtCacheSize) {
		this.prepStmtCacheSize = prepStmtCacheSize;
	}

	/**
	 * Internal prepared statement model with counter for improved overflow handling.
	 */
	public static class CassandraPreparedStatement {

		/** The counter how many times this prepared statement was requested */
		private AtomicInteger counter = new AtomicInteger(0);

		/** The backed prepared statement */
		private PreparedStatement preparedStatement;

		/**
		 * 
		 * @param preparedStament
		 */
		public CassandraPreparedStatement(PreparedStatement preparedStatement) {
			this.preparedStatement = preparedStatement;
		}

		/**
		 * Increase the counter and return the prepared statement.
		 * 
		 * @return
		 */
		public PreparedStatement useAndGet() {
			use();
			return preparedStatement;
		}

		/**
		 * Increases the counter for this prepared statement. If it hits Integer.MAX_VALUE it will freeze to that
		 * number.
		 * 
		 * @return
		 */
		public int use() {
			// Check for max and do not let it overflow to MIN_VALUE. If it is
			// used that much, it deserves to stay at max :)
			if(counter.get() == Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;

			} else {
				return counter.incrementAndGet();
			}
		}

		/**
		 * 
		 * @return
		 */
		public int getUsage() {
			return counter.get();
		}

		/**
		 * 
		 * @return
		 */
		public PreparedStatement getPreparedStatement() {
			return preparedStatement;
		}
	}
}
