package fr.liglab.lcm.tests;

import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ItemsetsFactory;
import fr.liglab.lcm.tests.stubs.EmptyInputFile;
import gnu.trove.iterator.TIntIterator;


public class ConcatenatedDatasetTest {
	
	@Test
	public void testNoCrashOnEmpty() {
		// super-high minimum support
		ConcatenatedDataset dataset = new ConcatenatedDataset(99999, FileReaderTest.getMicroReader());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().hasNext());
		// OR empty file
		dataset = new ConcatenatedDataset(1, new EmptyInputFile());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().hasNext());
	}

	@Test
	public void testLoadedMicro() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2, FileReaderTest.getMicroReader());

		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertEquals(5, dataset.getTransactionsCount());
		
		TIntIterator candidates = dataset.getCandidatesIterator();
		ItemsetsFactory factory = new ItemsetsFactory();
		
		while(candidates.hasNext()) {
			factory.add(candidates.next());
		}
		
		assertThat(factory.get(), arrayIsItemset(new int[] {3,5,6,7}));
	}
	
	@Test
	public void testClosureAtStart() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2, FileReaderTest.getGlobalClosure());
		
		int[] closure = dataset.getDiscoveredClosureItems();
		assertEquals(1, closure.length);
		assertEquals(1, closure[0]);
		assertEquals(5, dataset.getTransactionsCount());
		
		TIntIterator candidates = dataset.getCandidatesIterator();
		assertTrue(candidates.hasNext());
		assertEquals(2, candidates.next());
		assertFalse(candidates.hasNext());
		
		Dataset dataset2 = dataset.getProjection(2);
		assertEquals(0, dataset2.getDiscoveredClosureItems().length);
		assertFalse(dataset2.getCandidatesIterator().hasNext());
	}
	
	@Test
	public void testFakeClosure() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2, FileReaderTest.getFakeGlobalClosure());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		
		TIntIterator candidates = dataset.getCandidatesIterator();
		assertTrue(candidates.hasNext());
		assertEquals(1, candidates.next());
		assertTrue(candidates.hasNext());
		assertEquals(2, candidates.next());
		assertFalse(candidates.hasNext());
	}
	
	@Test
	public void testProjectionsOnMicro() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2, FileReaderTest.getMicroReader());
		
		//// STARTER : 3 - as min item, it only output itself 
		Dataset dataset3 = dataset.getProjection(3);
		assertEquals(0, dataset3.getDiscoveredClosureItems().length);
		
		TIntIterator candidates3 = dataset3.getCandidatesIterator();
		assertTrue(candidates3.hasNext());
		assertEquals(2, candidates3.next());
		assertFalse(candidates3.hasNext());
		
		//// STARTER : 5
		Dataset dataset5 = dataset.getProjection(5);
		assertEquals(0, dataset5.getDiscoveredClosureItems().length);
		assertFalse(dataset5.getCandidatesIterator().hasNext());
		
		//// STARTER : 6
		Dataset dataset6 = dataset.getProjection(6);
		assertThat(dataset6.getDiscoveredClosureItems(), arrayIsItemset(new int[] {1,3,5}));
		assertFalse(dataset6.getCandidatesIterator().hasNext());
		
		//// STARTER : 7 - candidates = 3,5,6
		Dataset dataset7 = dataset.getProjection(7);
		assertEquals(0, dataset7.getDiscoveredClosureItems().length);
		
		TIntIterator candidates7 = dataset7.getCandidatesIterator();
		assertTrue(candidates7.hasNext());
		assertEquals(3, candidates7.next());
		assertTrue(candidates7.hasNext());
		assertEquals(5, candidates7.next());
		assertTrue(candidates7.hasNext());
		assertEquals(6, candidates7.next());
		assertFalse(candidates7.hasNext());
		
		Dataset dataset7_3 = dataset7.getProjection(3);
		assertEquals(0, dataset7_3.getDiscoveredClosureItems().length);
		assertFalse(dataset7_3.getCandidatesIterator().hasNext());
		
		Dataset dataset7_5 = dataset7.getProjection(5);
		assertEquals(0, dataset7_5.getDiscoveredClosureItems().length);
		assertFalse(dataset7_5.getCandidatesIterator().hasNext());
		
		Dataset dataset7_6 = dataset7.getProjection(6);
		assertThat(dataset7_6.getDiscoveredClosureItems(), arrayIsItemset(new int[] {1,3,5}));
		assertFalse(dataset7_6.getCandidatesIterator().hasNext());
		
	}
}
