package fr.liglab.lcm.tests;

import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset;
import static org.junit.Assert.*;

import org.junit.Test;

import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.tests.stubs.EmptyInputFile;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.list.array.TIntArrayList;

public class ConcatenatedDatasetTest {

	@Test
	public void testNoCrashOnEmpty() {
		// super-high minimum support
		ConcatenatedDataset dataset = new ConcatenatedDataset(99999,
				FileReaderTest.getMicroReader());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().getExtension() != -1);
		// OR empty file
		dataset = new ConcatenatedDataset(1, new EmptyInputFile());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().getExtension() != -1);
	}

	@Test
	public void testLoadedMicro() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2,
				FileReaderTest.getMicroReader());

		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertEquals(5, dataset.getTransactionsCount());

		ExtensionsIterator candidates = dataset.getCandidatesIterator();
		ItemsetsFactory factory = new ItemsetsFactory();

		int c;
		while ((c = candidates.getExtension()) != -1) {
			factory.add(c);
		}

		assertThat(factory.get(), arrayIsItemset(new int[] { 3, 5, 6, 7 }));
	}

	@Test
	public void testClosureAtStart() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2,
				FileReaderTest.getGlobalClosure());

		int[] closure = dataset.getDiscoveredClosureItems();
		assertEquals(1, closure.length);
		assertEquals(1, closure[0]);
		assertEquals(5, dataset.getTransactionsCount());

		ExtensionsIterator candidates = dataset.getCandidatesIterator();
		assertEquals(2, candidates.getExtension());
		assertFalse(candidates.getExtension() != -1);

		Dataset dataset2 = dataset.getProjection(2);
		assertEquals(0, dataset2.getDiscoveredClosureItems().length);
		assertFalse(dataset2.getCandidatesIterator().getExtension() != -1);
	}

	@Test
	public void testFakeClosure() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2,
				FileReaderTest.getFakeGlobalClosure());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);

		ExtensionsIterator candidates = dataset.getCandidatesIterator();
		assertEquals(1, candidates.getExtension());
		assertEquals(2, candidates.getExtension());
		assertFalse(candidates.getExtension() != -1);
	}

	@Test
	public void testProjectionsOnMicro() {
		ConcatenatedDataset dataset = new ConcatenatedDataset(2,
				FileReaderTest.getMicroReader());

		// // STARTER : 3 - as min item, it only output itself
		Dataset dataset3 = dataset.getProjection(3);
		assertEquals(0, dataset3.getDiscoveredClosureItems().length);

		ExtensionsIterator candidates3 = dataset3.getCandidatesIterator();
		assertEquals(2, candidates3.getExtension());
		assertFalse(candidates3.getExtension() != -1);

		// // STARTER : 5
		Dataset dataset5 = dataset.getProjection(5);
		assertEquals(0, dataset5.getDiscoveredClosureItems().length);
		assertFalse(dataset5.getCandidatesIterator().getExtension() != -1);

		// // STARTER : 6
		Dataset dataset6 = dataset.getProjection(6);
		assertThat(dataset6.getDiscoveredClosureItems(),
				arrayIsItemset(new int[] { 1, 3, 5 }));
		assertFalse(dataset6.getCandidatesIterator().getExtension() != -1);

		// // STARTER : 7 - candidates = 3,5,6
		Dataset dataset7 = dataset.getProjection(7);
		assertEquals(0, dataset7.getDiscoveredClosureItems().length);

		ExtensionsIterator candidates7 = dataset7.getCandidatesIterator();
		assertEquals(3, candidates7.getExtension());
		assertEquals(5, candidates7.getExtension());
		assertEquals(6, candidates7.getExtension());
		assertFalse(candidates7.getExtension() != -1);

		Dataset dataset7_3 = dataset7.getProjection(3);
		assertEquals(0, dataset7_3.getDiscoveredClosureItems().length);
		assertFalse(dataset7_3.getCandidatesIterator().getExtension() != -1);

		Dataset dataset7_5 = dataset7.getProjection(5);
		assertEquals(0, dataset7_5.getDiscoveredClosureItems().length);
		assertFalse(dataset7_5.getCandidatesIterator().getExtension() != -1);

		Dataset dataset7_6 = dataset7.getProjection(6);
		assertThat(dataset7_6.getDiscoveredClosureItems(),
				arrayIsItemset(new int[] { 1, 3, 5 }));
		assertFalse(dataset7_6.getCandidatesIterator().getExtension() != -1);

	}
	
	/**
	 * Take care of assumptions ! (see isAincludedInB's javadoc)
	 */
	@Test
	public void testIsAIncludedInB() { 
		
		ConcatenatedDataset whatever = new ConcatenatedDataset(2,
				FileReaderTest.getMicroReader());

		TIntArrayList hole       = new TIntArrayList(new int[] {2,4});
		TIntArrayList small      = new TIntArrayList(new int[] {2,3,4});
		TIntArrayList smallPlus5 = new TIntArrayList(new int[] {2,3,4,5});
		TIntArrayList big        = new TIntArrayList(new int[] {2,3,4,5,6});
		TIntArrayList smallWith1 = new TIntArrayList(new int[] {1,2,3});
		TIntArrayList bigPlus1   = new TIntArrayList(new int[] {1,2,3,4,5,6});
		
		assertTrue(whatever.isAincludedInB(small, smallPlus5));
		
		assertFalse(whatever.isAincludedInB(smallWith1, small));
		assertFalse(whatever.isAincludedInB(small, smallWith1));
		
		assertTrue(whatever.isAincludedInB(big, bigPlus1));
		assertFalse(whatever.isAincludedInB(smallWith1, big));
		
		assertTrue(whatever.isAincludedInB(smallPlus5, bigPlus1));
		
		assertTrue(whatever.isAincludedInB(hole, small));
		assertTrue(whatever.isAincludedInB(hole, smallPlus5));
		assertTrue(whatever.isAincludedInB(hole, bigPlus1));
		assertFalse(whatever.isAincludedInB(hole, smallWith1));
	}
}
