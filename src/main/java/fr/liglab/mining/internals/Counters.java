package fr.liglab.mining.internals;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import fr.liglab.mining.io.PerItemTopKCollector;

/**
 * This class' constructor performs item counting over a transactions database,
 * and then gives access to various counters. It ignores items below the minimum
 * support. During recursions this allows the algorithm to choose what kind of
 * projections and filtering should be done, before instantiating the actual
 * projected dataset.
 * 
 * We're using arrays as Maps<Int,Int> , however they're not public as item
 * identifiers may have been renamed by dataset representation. By the way this
 * class is able to generate renamings (and applies them to itself by the way)
 * if you need to rename items in a future representation. You *MUST* handle
 * renaming after instantiation. See field reverseRenaming.
 */
public abstract class Counters implements Cloneable {

	/**
	 * Items occuring less than minSup times will be considered infrequent
	 */
	int minSupport;

	/**
	 * How many transactions are represented by the given dataset ?
	 */
	int transactionsCount;

	/**
	 * How many transactions have been counted (equals transactionsCount when
	 * all transactions have a weight of 1)
	 */
	int distinctTransactionsCount;

	/**
	 * Sum of given *filtered* transactions' lengths, ignoring their weight
	 */
	int distinctTransactionLengthSum;

	/**
	 * Items found to have a support count equal to transactionsCount (using IDs
	 * from given transactions) On renamed datasets you SHOULD NOT use
	 * getReverseRenaming to translate back these items, rather use parent's
	 * reverseRenaming (or none for the initial dataset)
	 */
	int[] closure;

	/**
	 * closure of parent's pattern UNION extension - using original item IDs
	 */
	int[] pattern;

	/**
	 * Counts how many items have a support count in [minSupport; 100%
	 */
	int nbFrequents;

	/**
	 * Biggest item ID having a support count in [minSupport; 100% [
	 */
	int maxFrequent;

	/**
	 * This array allows another class to output the discovered closure using
	 * original items' IDs.
	 * 
	 * After instanciation this field *must* be set by one of these methods -
	 * reuseRenaming, the initial dataset's constructor (which also sets
	 * "renaming") - compressRenaming, useful when recompacting dataset in
	 * recursions
	 */
	int[] reverseRenaming;

	/**
	 * Exclusive index of the first item >= core_item in current base
	 */

	int maxCandidate;

	/*
	 * careAboutFutureExtensions true for toplcm, false for baseline
	 */
	abstract public boolean raiseMinimumSupport(PerItemTopKCollector topKcoll, boolean careAboutFutureExtensions);

	abstract void eraseItem(int i);

	abstract public int insertUnclosedPatterns(PerItemTopKCollector topKcoll, boolean outputPatternsForFutureExtensions);

	// true if the current highest in topk can be considered unique
	static boolean updateTopK(int[] supports, int[] items, int item, int support, boolean highestUnique) {
		if (support < supports[0]) {
			return highestUnique;
		} else {
			int pos = Arrays.binarySearch(supports, support);
			if (pos < 0) {
				// pos = (-(insertion point) - 1)
				// first element greater
				// size of array if greatest
				int insertionPoint = -pos - 1;
				if (insertionPoint == 1) {
					supports[0] = support;
					items[0] = item;
				} else {
					System.arraycopy(supports, 1, supports, 0, insertionPoint - 1);
					System.arraycopy(items, 1, items, 0, insertionPoint - 1);
					supports[insertionPoint - 1] = support;
					items[insertionPoint - 1] = item;
				}
				if (insertionPoint == supports.length) {
					return true;
				} else {
					return highestUnique;
				}
			} else {
				if (pos != supports.length - 1) {
					return highestUnique;
				} else {
					return false;
				}
			}
		}
	}

	/**
	 * @return greatest frequent item's ID, which is also the greatest valid
	 *         index for arrays supportCounts and distinctTransactionsCounts
	 */
	public final int getMaxFrequent() {
		return this.maxFrequent;
	}

	/**
	 * @return a translation from internal item indexes to dataset's original
	 *         indexes
	 */
	public final int[] getReverseRenaming() {
		return this.reverseRenaming;
	}

	/**
	 * Will compress an older renaming, by removing infrequent items. Contained
	 * arrays (except closure) will refer new item IDs
	 * 
	 * @param olderReverseRenaming
	 *            reverseRenaming from the dataset that fed this Counter
	 * @return the translation from the old renaming to the compressed one
	 *         (gives -1 for removed items)
	 */
	abstract public int[] compressRenaming(int[] olderReverseRenaming);

	/**
	 * Will compress an older renaming, by removing infrequent items. Contained
	 * arrays (except closure) will refer new item IDs
	 * 
	 * @param olderReverseRenaming
	 *            reverseRenaming from the dataset that fed this Counter
	 * @param items
	 *            below this parameter will be renamed by decreasing frequency
	 * @return the translation from the old renaming to the compressed one
	 *         (gives -1 for removed items)
	 */
	abstract public int[] compressSortRenaming(int[] olderReverseRenaming);

	public final int getMaxCandidate() {
		return maxCandidate;
	}

	/**
	 * Notice: enumerated item IDs are in local base, use this.reverseRenaming
	 * 
	 * @return a thread-safe iterator over frequent items (in ascending order)
	 */
	public final FrequentsIterator getExtensionsIterator() {
		return new ExtensionsIterator(this.maxCandidate);
	}

	/**
	 * Notice: enumerated item IDs are in local base, use this.reverseRenaming
	 * 
	 * @return an iterator over frequent items (in ascending order)
	 */
	abstract public FrequentsIterator getLocalFrequentsIterator(final int from, final int to);

	/**
	 * Thread-safe iterator over frequent items (ie. those having a support
	 * count in [minSup, 100%[)
	 */
	protected class ExtensionsIterator implements FrequentsIterator {
		private final AtomicInteger index;
		private final int max;

		/**
		 * will provide an iterator on frequent items (in increasing order) in
		 * [0,to[
		 */
		public ExtensionsIterator(final int to) {
			this.index = new AtomicInteger(0);
			this.max = to;
		}

		/**
		 * @return -1 if iterator is finished
		 */
		public int next() {
			if (compactedRenaming()) {
				final int nextIndex = this.index.getAndIncrement();
				if (nextIndex < this.max) {
					return nextIndex;
				} else {
					return -1;
				}
			} else {
				while (true) {
					final int nextIndex = this.index.getAndIncrement();
					if (nextIndex < this.max) {
						if (getSupportCount(nextIndex) > 0) {
							return nextIndex;
						}
					} else {
						return -1;
					}
				}
			}
		}

		@Override
		public int peek() {
			return this.index.get();
		}

		@Override
		public int last() {
			return this.max;
		}
	}

	public final int getMinSupport() {
		return minSupport;
	}

	public final int getDistinctTransactionsCount() {
		return distinctTransactionsCount;
	}

	public final int getDistinctTransactionLengthSum() {
		return distinctTransactionLengthSum;
	}

	abstract public int getDistinctTransactionsCount(int item);

	abstract public int getSupportCount(int item);

	public final int getTransactionsCount() {
		return transactionsCount;
	}

	public final int[] getClosure() {
		return closure;
	}

	public final int[] getPattern() {
		return pattern;
	}

	public final int getNbFrequents() {
		return nbFrequents;
	}

	@Override
	abstract protected Counters clone();

	abstract public boolean compactedRenaming();

	static protected class FrequentIterator implements FrequentsIterator {

		private int index;
		private int max;
		private Counters supportsFilter;

		FrequentIterator() {
			this.max = 0;
			this.index = 0;
		}

		public void recycle(final int from, final int to, final Counters instance) {
			this.max = to;
			this.index = from;
			this.supportsFilter = instance.compactedRenaming() ? null : instance;
		}

		@Override
		public int next() {
			if (this.supportsFilter == null) {
				final int nextIndex = this.index++;
				if (nextIndex < this.max) {
					return nextIndex;
				} else {
					return -1;
				}
			} else {
				while (true) {
					final int nextIndex = this.index++;
					if (nextIndex < this.max) {
						if (supportsFilter.getSupportCount(nextIndex) > 0) {
							return nextIndex;
						}
					} else {
						return -1;
					}
				}
			}
		}

		@Override
		public int peek() {
			return this.index;
		}

		@Override
		public int last() {
			return this.max;
		}
	}
}
