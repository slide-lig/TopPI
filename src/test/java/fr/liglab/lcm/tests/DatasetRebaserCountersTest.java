package fr.liglab.lcm.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import fr.liglab.lcm.internals.DatasetRebaserCounters;
import fr.liglab.lcm.io.FileReader;

public class DatasetRebaserCountersTest {

	@Test
	public void testOnUnfiltering() {
		FileReader reader = new FileReader(FileReaderTest.PATH_TEST_UNFILTERING);
		DatasetRebaserCounters counters = new DatasetRebaserCounters(5, reader);
		assertEquals(16, counters.transactionsCount);
		assertEquals(30, counters.itemsRead);
		assertEquals(0, counters.rebaseMap.get(0));
		assertEquals(1, counters.rebaseMap.get(1));
		assertEquals(2, counters.rebaseMap.get(2));
		assertArrayEquals(new int[] {0,1,2} , counters.reverseMap);
	}

}
