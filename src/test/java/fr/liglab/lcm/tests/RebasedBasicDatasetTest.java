package fr.liglab.lcm.tests;

import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import fr.liglab.lcm.internals.BasicDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.RebasedBasicDataset;
import fr.liglab.lcm.tests.stubs.EmptyInputFile;
import gnu.trove.iterator.TIntIterator;

public class RebasedBasicDatasetTest {
	
	@Test
	public void testNoCrashOnEmpty() {
		// super-high minimum support
		BasicDataset dataset = new RebasedBasicDataset(99999, FileReaderTest.getMicroReader());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().hasNext());
		// OR empty file
		dataset = new RebasedBasicDataset(1, new EmptyInputFile());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().hasNext());
	}

	@Test
	public void testRemapping() {
		// rebasing.dat
		RebasedBasicDataset dataset = new RebasedBasicDataset(2, FileReaderTest.getRebasing());
		
		int[] reverse = dataset.getReverseMap();
		assertEquals(4, reverse.length);
		assertEquals(5, reverse[0]);
		assertEquals(4, reverse[1]);
		assertEquals(3, reverse[2]);
		assertEquals(2, reverse[3]);
		
		
		// globalclosure.dat
		dataset = new RebasedBasicDataset(2, FileReaderTest.getGlobalClosure());
		
		reverse = dataset.getReverseMap();
		assertEquals(2, reverse.length);
		assertEquals(1, reverse[0]);
		assertEquals(2, reverse[1]);
		
		
		//fakeglobalclosure.dat
		dataset = new RebasedBasicDataset(2, FileReaderTest.getFakeGlobalClosure());
		
		reverse = dataset.getReverseMap();
		assertEquals(2, reverse.length);
		assertEquals(1, reverse[0]);
		assertEquals(2, reverse[1]);
	}
	
	@Test
	public void testFileLoading() {
		RebasedBasicDataset dataset = new RebasedBasicDataset(2, FileReaderTest.getRebasing());
		
		assertEquals(6, dataset.getTransactionsCount());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
	}
	
	@Test
	public void testCandidateEnumeration() {
		RebasedBasicDataset dataset = new RebasedBasicDataset(2, FileReaderTest.getRebasing());
		TIntIterator iterator = dataset.getCandidatesIterator();
		
		// *rebased*
		assertTrue(iterator.hasNext());
		assertEquals(0, iterator.next()); // 5
		assertTrue(iterator.hasNext());
		assertEquals(1, iterator.next()); // 4
		assertTrue(iterator.hasNext());
		assertEquals(2, iterator.next()); // 3
		assertTrue(iterator.hasNext());
		assertEquals(3, iterator.next()); // 2
		assertFalse(iterator.hasNext());
		
		Dataset dataset_5 = dataset.getProjection(0);
		assertEquals(5, dataset_5.getTransactionsCount());
		assertFalse(dataset_5.getCandidatesIterator().hasNext());
		
		Dataset dataset_4 = dataset.getProjection(1);
		assertEquals(4, dataset_4.getTransactionsCount());
		TIntIterator it4 = dataset_4.getCandidatesIterator();
		assertTrue(it4.hasNext());
		assertEquals(0, it4.next());
		assertFalse(it4.hasNext());
		
		Dataset dataset_3 = dataset.getProjection(2);
		assertEquals(3, dataset_3.getTransactionsCount());
		assertThat(dataset_3.getDiscoveredClosureItems(), arrayIsItemset(new int[] {0}));
		assertFalse(dataset_3.getCandidatesIterator().hasNext());
		
		Dataset dataset_2 = dataset.getProjection(3);
		assertEquals(2, dataset_2.getTransactionsCount());
		assertThat(dataset_2.getDiscoveredClosureItems(), arrayIsItemset(new int[] {0,2}));
		assertFalse(dataset_2.getCandidatesIterator().hasNext());
	}

}
