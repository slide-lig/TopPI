package fr.liglab.lcm.internals.nomaps;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import fr.liglab.lcm.internals.FrequentsIterator;
import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.util.ItemsetsFactory;

/**
 * Item counting and all other stats available from a single pass
 * 
 * Once constructed, all of these are available in public fields
 */
public class DatasetCountersRenamer {

	/**
	 * Anything with a support count below this value will be considered
	 * unfrequent (and, sooner or later, removed)
	 */
	public final int minSup;

	/**
	 * How many transactions have been counted
	 */
	public final int transactionsCount;

	/**
	 * How many items have been read from inputted transactions
	 */
	public final int itemsRead;

	/**
	 * Values of supportCounts, summed
	 */
	public final int supportsSum;
	public final int distinctTransactionLengthSum;
	/**
	 * Support count for each item having a support count in [minSup; 100% [
	 */
	public final int[] supportCounts;
	public final int[] distinctTransactionsCounts;

	/**
	 * Items found to have a 100% support count
	 */
	public final int[] closure;

	/**
	 * items having a support count in [minSup; 100% [, sorted
	 */
	public final int nbFrequents;
	public final int[] reverseRenaming;
	public final int[] renaming;

	/**
	 * @return an iterator on frequent items in ascending order, up to "max"
	 *         (excluded)
	 */
	public final FrequentsIterator getFrequentsIteratorTo(int max) {
		return new AllFrequentsIterator(max);
	}

	/**
	 * @return an iterator on frequent items (in ascending order)
	 */
	public final FrequentsIterator getFrequentsIterator() {
		return new AllFrequentsIterator(Integer.MAX_VALUE);
	}

	DatasetCountersRenamer(int minimumSupport, Iterator<TransactionReader> transactions, int maxItem) {
		this(minimumSupport, transactions, Integer.MIN_VALUE, null, maxItem);
	}

	/**
	 * Counts items appearing in provided transactions
	 * 
	 * @param minimumSupport
	 * @param transactions
	 * @param ingoreItem
	 *            avoids building an array when there's only extension to be
	 *            ignored
	 * @param ignoreItems
	 *            (may be null) items that may appear in transactions but should
	 *            to be counted
	 */
	DatasetCountersRenamer(int minimumSupport, Iterator<TransactionReader> transactions, int ingoredItem,
			int[] ignoredItems, final int maxItem) {
		this.minSup = minimumSupport;

		int lineCount = 0;
		int itemCount = 0;
		int[] tempTransactionsCounts = new int[maxItem];
		int[] tempDistinctTransactionsCount = new int[maxItem];
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			int weight = transaction.getTransactionSupport();
			lineCount++;

			while (transaction.hasNext()) {
				itemCount++;
				int item = transaction.next();
				tempTransactionsCounts[item] += weight;
				tempDistinctTransactionsCount[item]++;
			}
		}

		this.transactionsCount = lineCount;
		this.itemsRead = itemCount;

		tempTransactionsCounts[ingoredItem] = 0;
		tempDistinctTransactionsCount[ingoredItem] = 0;
		if (ignoredItems != null) {
			for (int item : ignoredItems) {
				tempTransactionsCounts[item] = 0;
				tempDistinctTransactionsCount[item] = 0;
			}
		}

		ItemsetsFactory builder = new ItemsetsFactory();
		int remainingSupportsSum = 0;
		int remainingDistinctTransLength = 0;
		int remainingFrequents = 0;
		int maxFrequent = -1;
		for (int i = 0; i < tempTransactionsCounts.length; i++) {
			if (tempTransactionsCounts[i] < minimumSupport) {
				tempTransactionsCounts[i] = 0;
				tempDistinctTransactionsCount[i] = 0;
			} else if (tempTransactionsCounts[i] == this.transactionsCount) {
				builder.add(i);
				tempTransactionsCounts[i] = 0;
				tempDistinctTransactionsCount[i] = 0;
			} else {
				maxFrequent = Math.max(maxFrequent, i);
				remainingFrequents++;
				remainingSupportsSum += tempTransactionsCounts[i];
				remainingDistinctTransLength += tempDistinctTransactionsCount[i];
			}
		}

		this.closure = builder.get();
		this.supportsSum = remainingSupportsSum;
		this.distinctTransactionLengthSum = remainingDistinctTransLength;
		this.nbFrequents = remainingFrequents;
		this.reverseRenaming = new int[this.nbFrequents];
		this.renaming = new int[maxFrequent];
		this.supportCounts = new int[this.nbFrequents];
		this.distinctTransactionsCounts = new int[this.nbFrequents];
		int insertPos = 0;
		for (int i = 0; i < tempTransactionsCounts.length; i++) {
			if (tempTransactionsCounts[i] > 0) {
				this.reverseRenaming[insertPos] = i;
				this.renaming[i] = insertPos;
				this.supportCounts[insertPos] = tempTransactionsCounts[i];
				this.distinctTransactionsCounts[insertPos] = tempTransactionsCounts[i];
			}
		}
	}

	public void updateReverseRenaming(int[] previousRenaming) {
		for (int i = 0; i < this.reverseRenaming.length; i++) {
			this.reverseRenaming[i] = previousRenaming[this.reverseRenaming[i]];
		}
		for (int i = 0; i < this.closure.length; i++) {
			this.closure[i] = previousRenaming[this.closure[i]];
		}
	}

	/**
	 * Thread-safe iterator over sortedFrequents
	 */
	protected class AllFrequentsIterator implements FrequentsIterator {
		private final AtomicInteger index;
		private final int max;

		/**
		 * will provide an iterator on frequent items in [0,to[
		 */
		public AllFrequentsIterator(final int to) {
			this.index = new AtomicInteger(0);
			this.max = to;
		}

		/**
		 * @return -1 if iterator is finished
		 */
		public int next() {
			final int nextIndex = this.index.getAndIncrement();
			if (nextIndex < this.max) {
				return this.max;
			} else {
				return -1;
			}
		}
	}
}
