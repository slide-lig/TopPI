package fr.liglab.lcm.tests;

import static fr.liglab.lcm.tests.matchers.ItemsetsAsArraysMatcher.arrayIsItemset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import fr.liglab.lcm.internals.BasicDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.tests.stubs.EmptyInputFile;
import fr.liglab.lcm.util.ItemsetsFactory;

public class BasicDatasetTest {

	@Test
	public void testNoCrashOnEmpty() {
		// super-high minimum support
		BasicDataset dataset = new BasicDataset(99999,
				FileReaderTest.getMicroReader());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().getExtension() != -1);
		// OR empty file
		dataset = new BasicDataset(1, new EmptyInputFile());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);
		assertFalse(dataset.getCandidatesIterator().getExtension() != -1);
	}

	@Test
	public void testLoadedMicro() {
		BasicDataset dataset = new BasicDataset(2,
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
		BasicDataset dataset = new BasicDataset(2,
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
		BasicDataset dataset = new BasicDataset(2,
				FileReaderTest.getFakeGlobalClosure());
		assertEquals(0, dataset.getDiscoveredClosureItems().length);

		ExtensionsIterator candidates = dataset.getCandidatesIterator();
		assertEquals(1, candidates.getExtension());
		assertEquals(2, candidates.getExtension());
		assertFalse(candidates.getExtension() != -1);
	}

	@Test
	public void testProjectionsOnMicro() {
		BasicDataset dataset = new BasicDataset(2,
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
}
