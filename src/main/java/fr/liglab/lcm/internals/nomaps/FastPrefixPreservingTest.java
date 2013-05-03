package fr.liglab.lcm.internals.nomaps;

import fr.liglab.lcm.internals.tidlist.TidList;
import gnu.trove.iterator.TIntIterator;

final class FastPrefixPreservingTest {

	/**
	 * @return the biggest j > extension having the same support, or -1 if such
	 *         a j does not exist
	 */
	public static int ppTest(int extension, DatasetCountersRenamer counters, TidList occurrences) {

		final int candidateSupport = counters.supportCounts[extension];

		for (int i = counters.nbFrequents - 1; i > extension; i--) {
			if (counters.supportCounts[i] >= candidateSupport) {
				final TIntIterator candidateOccurrences = occurrences.get(extension);
				final TIntIterator jOccurrences = occurrences.get(i);
				if (isAincludedInB(candidateOccurrences, jOccurrences)) {
					return i;
				}
			}
		}

		return -1;
	}

	private static boolean isAincludedInB(final TIntIterator aIt, final TIntIterator bIt) {
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
