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

	public AllFrequentsIterator extensionsIterator;
	
	/**
	 * Sum of counted transactions' weights
	 */
	public final int transactionsCount;
	
	/**
	 * Values of supportCounts, summed
	 */
	public final int supportsSum;
	public final int distinctTransactionLengthSum;
	/**
	 * Support count for each item having a support count in [minSup; 100% [
	 */
	public int[] supportCounts;
	public int[] distinctTransactionsCounts;

	/**
	 * Items found to have a 100% support count
	 */
	public final int[] closure;

	/**
	 * items having a support count in [minSup; 100% [, sorted
	 */
	public final int nbFrequents;
	public int[] reverseRenaming;
	public int[] renaming;

	private int maxFrequent;

	private final int extension;

	DatasetCountersRenamer(int minimumSupport, Iterator<TransactionReader> transactions, int maxItem) {
		this(minimumSupport, transactions, Integer.MAX_VALUE, null, maxItem, false);
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
	DatasetCountersRenamer(int minimumSupport, Iterator<TransactionReader> transactions, int extension,
			int[] ignoredItems, final int maxItem, boolean compactRebase) {
		// fix fake extensions
		this.extension = Math.min(extension, maxItem + 1);
		this.minSup = minimumSupport;
		int weightsSum = 0;
		this.supportCounts = new int[maxItem];
		this.distinctTransactionsCounts = new int[maxItem];
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			int weight = transaction.getTransactionSupport();
			
			if (transaction.hasNext()) {
				weightsSum += weight;
			}

			while (transaction.hasNext()) {
				int item = transaction.next();
				this.supportCounts[item] += weight;
				this.distinctTransactionsCounts[item]++;
			}
		}

		this.transactionsCount = weightsSum;
		if (extension != Integer.MAX_VALUE) {
			this.supportCounts[extension] = 0;
			this.distinctTransactionsCounts[extension] = 0;
		}
		if (ignoredItems != null) {
			for (int item : ignoredItems) {
				this.supportCounts[item] = 0;
				this.distinctTransactionsCounts[item] = 0;
			}
		}
		ItemsetsFactory builder = new ItemsetsFactory();
		int remainingSupportsSum = 0;
		int remainingDistinctTransLength = 0;
		int remainingFrequents = 0;
		this.maxFrequent = -1;
		for (int i = 0; i < this.supportCounts.length; i++) {
			if (this.supportCounts[i] < minimumSupport) {
				this.supportCounts[i] = 0;
				this.distinctTransactionsCounts[i] = 0;
			} else if (this.supportCounts[i] == this.transactionsCount) {
				builder.add(i);
				this.supportCounts[i] = 0;
				this.distinctTransactionsCounts[i] = 0;
			} else {
				this.maxFrequent = Math.max(this.maxFrequent, i);
				remainingFrequents++;
				remainingSupportsSum += this.supportCounts[i];
				remainingDistinctTransLength += this.distinctTransactionsCounts[i];
			}
		}

		this.closure = builder.get();
		this.supportsSum = remainingSupportsSum;
		this.distinctTransactionLengthSum = remainingDistinctTransLength;
		this.nbFrequents = remainingFrequents;
		this.reverseRenaming = null;
		this.renaming = null;
	}

	public void compactRebase(boolean rebase, int[] previousRenaming) {
		if (rebase) {
			int[] newSupportCounts = new int[this.nbFrequents];
			int[] newDistinctTransactionsCount = new int[this.nbFrequents];
			this.reverseRenaming = new int[this.nbFrequents];
			this.renaming = new int[maxFrequent];
			int insertPos = 0;
			int maxExtension = -1;
			for (int i = 0; i < this.supportCounts.length; i++) {
				if (this.supportCounts[i] > 0) {
					if (i < extension) {
						maxExtension = insertPos;
					}
					if (previousRenaming == null) {
						this.reverseRenaming[insertPos] = i;
					} else {
						this.reverseRenaming[insertPos] = previousRenaming[this.reverseRenaming[i]];
					}
					this.renaming[i] = insertPos;
					newSupportCounts[insertPos] = this.supportCounts[i];
					newDistinctTransactionsCount[insertPos] = this.distinctTransactionsCounts[i];
				}
			}
			this.supportCounts = newSupportCounts;
			this.distinctTransactionsCounts = newDistinctTransactionsCount;
			this.extensionsIterator = new AllFrequentsIterator(maxExtension);
		} else {
			this.extensionsIterator = new AllFrequentsIterator(this.extension);
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
			if (renaming == null) {
				while (true) {
					final int nextIndex = this.index.getAndIncrement();
					if (nextIndex < this.max) {
						if (supportCounts[nextIndex] > 0) {
							return nextIndex;
						}
					} else {
						return -1;
					}
				}
			} else {
				final int nextIndex = this.index.getAndIncrement();
				if (nextIndex < this.max) {
					return nextIndex;
				} else {
					return -1;
				}
			}
		}
	}
}
