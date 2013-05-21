package fr.liglab.lcm.tests;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import fr.liglab.lcm.internals.nomaps.Counters;
import fr.liglab.lcm.internals.nomaps.Dataset.TransactionsIterable;
import fr.liglab.lcm.internals.nomaps.ExplorationStep;

public class CompactionTest {

	@Test
	public void test() {
		
		ExplorationStep init = new ExplorationStep(2, FileReaderTest.PATH_MICRO);
		
		TransactionsIterable support = init.dataset.getSupport(1);
		Counters candidateCounts = new Counters(2, support.iterator(), 
				1, null, 5);
		System.out.println(Arrays.toString(init.counters.getReverseRenaming()));
		int[] renaming = candidateCounts.compressRenaming(init.counters.getReverseRenaming());
		
		System.out.println(Arrays.toString(renaming));
		
	}

}
