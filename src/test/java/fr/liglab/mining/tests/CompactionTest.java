package fr.liglab.mining.tests;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import fr.liglab.mining.internals.Counters;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.Dataset.TransactionsIterable;

public class CompactionTest {

	@Test
	public void test() {
		
		ExplorationStep init = new ExplorationStep(2, FileReaderTest.PATH_MICRO);
		
		TransactionsIterable support = init.getDataset().getSupport(1);
		Counters candidateCounts = new Counters(2, support.iterator(), 
				1, null, 5);
		System.out.println(Arrays.toString(init.counters.getReverseRenaming()));
		int[] renaming = candidateCounts.compressRenaming(init.counters.getReverseRenaming());
		
		System.out.println(Arrays.toString(renaming));
		
	}

}
