package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Arrays;


final class FastPrefixPreservingTest {
	
	/**
	 * @return the biggest j > extension having the same support, or -1 if such a j does not exist
	 */
	public static int ppTest(int extension, DatasetCounters counters, 
			TIntObjectHashMap<TIntArrayList> occurrences) {
		
		final int candidateIndex = Arrays.binarySearch(counters.sortedFrequents, extension);
		final int[] frequentItems = counters.sortedFrequents;
		final TIntIntMap supportCounts = counters.supportCounts;
		
		final int candidate = frequentItems[candidateIndex];
		final int candidateSupport = supportCounts.get(candidate);
		final TIntArrayList candidateOccurrences = occurrences.get(candidate);
		
		for (int i = frequentItems.length - 1; i > candidateIndex ; i--) {
			int j = frequentItems[i];

			if (supportCounts.get(j) >= candidateSupport) {
				TIntArrayList jOccurrences = occurrences.get(j);
				if (isAincludedInB(candidateOccurrences, jOccurrences)) {
					return j;
				}
			}
		}

		return -1;
	}

	/**
	 * Assumptions : - both contain array indexes appended in increasing order -
	 * you already tested that B.size >= A.size
	 * 
	 * @return true if A is included in B
	 */
	private static boolean isAincludedInB(final TIntArrayList a, final TIntArrayList b) {
		TIntIterator aIt = a.iterator();
		TIntIterator bIt = b.iterator();

		int tidA = 0;
		int tidB = 0;

		while (aIt.hasNext() && bIt.hasNext()) {
			tidA = aIt.next();
			tidB = bIt.next();

			while (tidB < tidA && bIt.hasNext()) {
				tidB = bIt.next();
			}

			if (tidB > tidA) {
				return false;
			}
		}

		return tidA == tidB && !aIt.hasNext();
	}
}
