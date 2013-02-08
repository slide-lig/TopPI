package fr.liglab.lcm.tests;

import static org.junit.Assert.*;
import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset;

import org.junit.Test;

import fr.liglab.lcm.internals.BasicDataset;
import fr.liglab.lcm.internals.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;

/**
 * For every implementation of Dataset, add 
 */
public class BasicDatasetTest {

	@Test
	public void testLoadedMicro() {
		BasicDataset dataset = new BasicDataset(2, FileReaderTest.getMicroReader());

		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertEquals(5, dataset.getTransactionsCount());
		
		TIntIterator candidates = dataset.getCandidatesIterator();
		ItemsetsFactory factory = new ItemsetsFactory();
		
		while(candidates.hasNext()) {
			factory.add(candidates.next());
		}
		
		assertThat(factory.get(), arrayIsItemset(new int[] {1,2,3,5,7}));
	}
}
