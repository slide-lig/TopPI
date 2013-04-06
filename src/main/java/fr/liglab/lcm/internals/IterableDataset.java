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

	protected interface TransactionReader extends TIntIterator {
		public int getTransactionSupport();

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
}
