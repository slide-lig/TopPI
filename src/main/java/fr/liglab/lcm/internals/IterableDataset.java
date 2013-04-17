package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class IterableDataset extends Dataset {
	protected int[] frequentItems;

	IterableDataset(int minimum_support, int core_item) {
		super(minimum_support, core_item);
	}

	protected abstract TransactionReader readTransaction(int tid);

	protected abstract TIntList getTidList(int item);

	// true if transactions have their items sorted in increasing order
	public abstract boolean itemsSorted();

	protected interface TransactionReader {
		public int getTransactionSupport();

		public boolean hasNext();

		public int next();

	}

	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}

	protected class CandidatesIterator implements ExtensionsIterator {
		private AtomicInteger next_index;
		private final int candidatesLength; // candidates is
											// frequentItems[0:candidatesLength[

		public int[] getSortedFrequents() {
			return frequentItems;
		}

		public CandidatesIterator() {
			this.next_index = new AtomicInteger(-1);

			int coreItemIndex = Arrays.binarySearch(frequentItems, coreItem);
			if (coreItemIndex >= 0) {
				throw new RuntimeException("Unexpected : coreItem appears in frequentItems !");
			}
			candidatesLength = -coreItemIndex - 1;
		}

		public int getExtension() {
			if (candidatesLength < 0) {
				return -1;
			}
			while (true) {
				int next_index_local = this.next_index.incrementAndGet();
				if (next_index_local < 0) {
					// overflow, just in case
					return -1;
				}
				if (next_index_local >= this.candidatesLength) {
					return -1;
				} else {
					return frequentItems[next_index_local];
				}
			}
		}
	}

	/**
	 * @return greatest j > candidate having the same support as candidate, -1
	 *         if not such item exists
	 */
	public int prefixPreservingTest(final int candidate) {
		int candidateIdx = Arrays.binarySearch(frequentItems, candidate);
		if (candidateIdx < 0) {
			throw new RuntimeException("Unexpected : prefixPreservingTest of an infrequent item, " + candidate);
		}

		return ppTest(candidateIdx);
	}

	/**
	 * @return greatest j > candidate having the same support as candidate at
	 *         the given index, -1 if not such item exists
	 */
	protected int ppTest(final int candidateIndex) {
		final int candidate = frequentItems[candidateIndex];
		final int candidateSupport = supportCounts.get(candidate);
		TIntList candidateOccurrences = this.getTidList(candidate);

		for (int i = frequentItems.length - 1; i > candidateIndex; i--) {
			int j = frequentItems[i];

			if (supportCounts.get(j) >= candidateSupport) {
				TIntList jOccurrences = this.getTidList(j);
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
	public boolean isAincludedInB(final TIntList a, final TIntList b) {
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
