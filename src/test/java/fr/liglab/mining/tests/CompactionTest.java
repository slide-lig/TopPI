package fr.liglab.mining.tests;

import org.junit.Test;

import fr.liglab.mining.internals.Counters;
import fr.liglab.mining.internals.Dataset.TransactionsIterable;
import fr.liglab.mining.internals.DenseCounters;
import fr.liglab.mining.internals.ExplorationStep;

public class CompactionTest {

	@Test
	public void test() {
		
		ExplorationStep init = new ExplorationStep(2, FileReaderTest.PATH_MICRO, 10);
		
		TransactionsIterable support = init.dataset.getSupport(1);
		Counters candidateCounts = new DenseCounters(2, support.iterator(), 
				1, null, 5, init.counters.getReverseRenaming(), new int[] {});
		int[] renaming = candidateCounts.compressRenaming(init.counters.getReverseRenaming());
		
	}

}
