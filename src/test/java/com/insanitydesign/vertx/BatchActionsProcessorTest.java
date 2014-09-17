package com.insanitydesign.vertx;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.insanitydesign.vertx.BatchActionsProcessor;

/**
 * 
 * @author insanitydesign
 */
public class BatchActionsProcessorTest {

	/**
	 * 
	 */
	@Test
	public void testAddress() {
		BatchActionsProcessor batchActionsProcessor = new BatchActionsProcessor();

		//
		String testAddress1 = "testing.the.processor";
		batchActionsProcessor.setAddress(testAddress1);

		assertEquals(testAddress1 + ".batch", batchActionsProcessor.getAddress());
		assertEquals(testAddress1, batchActionsProcessor.getPersistorAddress());

		//
		String testAddress2 = "testing-the-processor.";
		batchActionsProcessor.setAddress(testAddress2);

		assertEquals(testAddress2.substring(0, testAddress2.length() - 1) + ".batch", batchActionsProcessor.getAddress());
		assertEquals(testAddress2, batchActionsProcessor.getPersistorAddress());
	}	
}
