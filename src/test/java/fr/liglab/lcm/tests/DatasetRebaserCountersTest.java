package fr.liglab.lcm.tests;

import org.junit.Test;
import static org.junit.Assert.*;

import fr.liglab.lcm.internals.DatasetRebaserCounters;

public class DatasetRebaserCountersTest {

	@Test
	public void testOnUnfiltering() {
		DatasetRebaserCounters counters = new DatasetRebaserCounters(5, FileReaderTest.getTestUnfiltering());
		assertEquals(14, counters.transactionsCount);
		assertEquals(67, counters.itemsRead);
		
	}

}
