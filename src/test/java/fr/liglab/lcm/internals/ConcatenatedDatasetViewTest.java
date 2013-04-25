package fr.liglab.lcm.internals;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import fr.liglab.lcm.internals.DatasetFactory.DontExploreThisBranchException;
import fr.liglab.lcm.tests.FileReaderTest;

public class ConcatenatedDatasetViewTest {

	@Test
	public void testCounting() throws DontExploreThisBranchException {
		Dataset root = DatasetFactory.fromFile(2, FileReaderTest.PATH_TEST_UNFILTERING);
		
		Dataset projected_2 = DatasetFactory.project(root, 2);
		DatasetCounters counters_2 = projected_2.counters;
		assertArrayEquals(new int[] {0}, counters_2.closure);
		assertArrayEquals(new int[] {1}, counters_2.sortedFrequents);
		
		FrequentsIterator it_2 = counters_2.getFrequentsIteratorTo(2);
		assertEquals(1, it_2.next());
		assertEquals(-1, it_2.next());
		
		Dataset projected_2_1 = DatasetFactory.project(projected_2, 1);
		
		assertTrue(projected_2_1 instanceof ConcatenatedDatasetView);
		ConcatenatedDatasetView casted = (ConcatenatedDatasetView) projected_2_1;
		assertEquals(root.counters.transactionsCount, casted.getConcatenatedTransactionCount(), 1);
	}

}
