package com.insanitydesign.vertx;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.vertx.java.core.logging.Logger;

import com.datastax.driver.core.Session;
import com.insanitydesign.vertx.PreparedStatementCache;
import com.insanitydesign.vertx.PreparedStatementCache.CassandraPreparedStatement;

/**
 * 
 * @author insanitydesign
 */
public class PreparedStatementCacheTest {

	/** */
	@Mock
	private Session session;
	
	/** */
	@Mock
	private Logger logger;
	
	/**
	 * 
	 */
	@Before
	public void init() {
		//Mock the convenience
		this.session = mock(Session.class);
		when(this.session.prepare(any(String.class))).thenReturn(null);
		
		this.logger = mock(Logger.class);
		when(this.logger.isDebugEnabled()).thenReturn(true);
	}
	
	/**
	 * 
	 */
	@Test
	public void testCache() {
		//Create cache to test on
		PreparedStatementCache preparedStatementCache = new PreparedStatementCache(1, session, logger);
		
		//Test
		String statement = "SELECT * FROM test.table";
		preparedStatementCache.add(statement);
		
		CassandraPreparedStatement prepStmt = preparedStatementCache.get(statement);
		assertNotNull(prepStmt);
		assertEquals(prepStmt.getUsage(), 0);
		assertEquals(preparedStatementCache.getCachedStatements().size(), 1);
	}
		
	/**
	 * 
	 */
	@Test
	public void testIncrementCache() {
		//Create cache to test on
		PreparedStatementCache preparedStatementCache = new PreparedStatementCache(1, session, logger);
		
		//Test
		String statement = "SELECT * FROM test.table";
		preparedStatementCache.addAndGet(statement);
		
		CassandraPreparedStatement prepStmt = preparedStatementCache.get(statement);
		assertNotNull(prepStmt);
		assertEquals(prepStmt.getUsage(), 1);
		assertEquals(preparedStatementCache.getCachedStatements().size(), 1);
	}
	
	/**
	 * 
	 */
	@Test
	public void testMultiIncrementCache() {
		//Create cache to test on
		PreparedStatementCache preparedStatementCache = new PreparedStatementCache(1, session, logger);
		
		//Test
		String statement = "SELECT * FROM test.table";
		int amount = 10;
		for(int i = 0; i < amount; i++) {
			preparedStatementCache.addAndGet(statement);	
		}
		
		CassandraPreparedStatement prepStmt = preparedStatementCache.get(statement);
		assertNotNull(prepStmt);
		assertEquals(prepStmt.getUsage(), amount);
		assertEquals(preparedStatementCache.getCachedStatements().size(), 1);
	}
	
	/**
	 * 
	 */
	@Test
	public void testCacheSize() {
		//Create cache to test on
		PreparedStatementCache preparedStatementCache = new PreparedStatementCache(1, session, logger);
		
		//Test
		String statement = "SELECT * FROM test.table";
		preparedStatementCache.addAndGet(statement);
		
		CassandraPreparedStatement prepStmt = preparedStatementCache.get(statement);
		assertNotNull(prepStmt);
		assertEquals(prepStmt.getUsage(), 1);
		
		//New one replaces old and therefore null
		String statement2 = "SELECT * FROM test.table2";
		preparedStatementCache.addAndGet(statement2);
		
		prepStmt = preparedStatementCache.get(statement);
		assertNull(prepStmt);
		//But the new one is now present
		prepStmt = preparedStatementCache.get(statement2);
		assertNotNull(prepStmt);
		assertEquals(prepStmt.getUsage(), 1);
		assertEquals(preparedStatementCache.getCachedStatements().size(), 1);
	}
	
	/**
	 * 
	 */
	@Test
	public void testBiggerCacheSize() {
		//Create cache to test on
		int cacheSize = 100;
		PreparedStatementCache preparedStatementCache = new PreparedStatementCache(cacheSize, session, logger);
		
		//Test
		String statement = "SELECT * FROM test.table";
		for(int i = 0; i < cacheSize + 10; i++) {
			preparedStatementCache.addAndGet(statement + i);
		}
		
		//
		assertEquals(preparedStatementCache.getCachedStatements().size(), cacheSize);		
	}
	
	/**
	 * 
	 */
	@Test
	public void testStatementReplacement() {
		//Create cache to test on
		int cacheSize = 100;
		PreparedStatementCache preparedStatementCache = new PreparedStatementCache(cacheSize, session, logger);
		
		//Test
		String statement = "SELECT * FROM test.table";
		for(int i = 0; i < cacheSize; i++) {
			preparedStatementCache.addAndGet(statement + i);
		}

		CassandraPreparedStatement prepStmt = preparedStatementCache.get(statement + (cacheSize - 1));
		assertNotNull(prepStmt);
		assertEquals(prepStmt.getUsage(), 1);
		
		//Multiple use of 99
		for(int i = 0; i < cacheSize - 1; i++) {
			preparedStatementCache.addAndGet(statement + i);
		}

		//
		for(int i = 0; i < cacheSize - 1; i++) {
			prepStmt = preparedStatementCache.get(statement + i);
			assertNotNull(prepStmt);
			assertEquals(prepStmt.getUsage(), 2);
		}
		
		//Adding original one replacing the least used
		preparedStatementCache.addAndGet(statement);
		//
		prepStmt = preparedStatementCache.get(statement + (cacheSize - 1));
		assertNull(prepStmt);
		
		//
		assertEquals(preparedStatementCache.getCachedStatements().size(), cacheSize);		
	}
}
