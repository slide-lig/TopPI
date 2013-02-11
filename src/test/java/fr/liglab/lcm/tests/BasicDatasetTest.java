package fr.liglab.lcm.tests;

import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset;
import static org.junit.Assert.*;

import org.junit.Test;

import fr.liglab.lcm.internals.BasicDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;


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
	
	@Test
	public void testClosureAtStart() {
		BasicDataset dataset = new BasicDataset(3, FileReaderTest.getGlobalClosure());
		
		int[] closure = dataset.getDiscoveredClosureItems();
		assertEquals(1, closure.length);
		assertEquals(1, closure[0]);
		assertEquals(5, dataset.getTransactionsCount());
		
		TIntIterator candidates = dataset.getCandidatesIterator();
		assertTrue(candidates.hasNext());
		assertEquals(2, candidates.next());
		assertFalse(candidates.hasNext());
	}
	
	@Test
	public void testFakeClosure() {
		BasicDataset dataset = new BasicDataset(4, FileReaderTest.getFakeGlobalClosure());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		
		TIntIterator candidates = dataset.getCandidatesIterator();
		while(candidates.hasNext()) {
			assertFalse(1 != candidates.next());
		}
	}
	
	@Test
	public void testProjectionsOnMicro() {
		BasicDataset dataset = new BasicDataset(2, FileReaderTest.getMicroReader());
		
		//// STARTER : 1
		Dataset dataset1 = dataset.getProjection(1);
		assertThat(dataset1.getDiscoveredClosureItems(), arrayIsItemset(new int[] {3,5,6}));
		TIntIterator candidates1 = dataset1.getCandidatesIterator();
		assertTrue(candidates1.hasNext());
		assertEquals(7, candidates1.next());
		assertFalse(candidates1.hasNext());

		Dataset dataset1_7 = dataset1.getProjection(7);
		assertEquals(0, dataset1_7.getDiscoveredClosureItems().length);
		
		//// STARTER : 2
		Dataset dataset2 = dataset.getProjection(2);
		assertThat(dataset2.getDiscoveredClosureItems(), arrayIsItemset(new int[] {3}));
		assertFalse(dataset2.getCandidatesIterator().hasNext());
		
		//// STARTER : 3
		Dataset dataset3 = dataset.getProjection(3);
		assertEquals(0, dataset3.getDiscoveredClosureItems().length);
		TIntIterator candidates3 = dataset3.getCandidatesIterator();
		assertTrue(candidates3.hasNext());
		assertEquals(7, candidates3.next());
		assertFalse(candidates3.hasNext());
		
		// 5 should behave as 3
		
		//// STARTER : 7 - the max item should only output itself
		Dataset dataset7 = dataset.getProjection(7);
		assertEquals(0, dataset7.getDiscoveredClosureItems().length);
		assertFalse(dataset2.getCandidatesIterator().hasNext());
	}
}
