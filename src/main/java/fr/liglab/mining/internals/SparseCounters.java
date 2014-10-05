package fr.liglab.mining.internals;

import fr.liglab.mining.CountersHandler;
import fr.liglab.mining.CountersHandler.TopPICounters;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.util.ItemsetsFactory;
import gnu.trove.impl.Constants;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Arrays;
import java.util.Iterator;

public class SparseCounters extends Counters {
	/**
	 * Support count, per item having a support count in [minSupport; 100% [
	 * Items having a support count below minSupport are considered infrequent,
	 * those at 100% belong to closure, for both supportCounts[i] = 0 - except
	 * if renaming happened, in which case such items no longer exists.
	 * 
	 * Indexes above maxFrequent should be considered valid.
	 * 
	 */
	private TIntIntMap supportCounts;

	private int[] rebasedSupportCounts;

	private TIntIntMap distinctTransactionsCounts;

	private int[] rebasedDistinctTransactionsCounts;
	/**
	 * will be set to true if arrays have been compacted, ie. if supportCounts
	 * and distinctTransactionsCounts don't contain any zero.
	 */
	private boolean compactedArrays = false;

	private final int rebasingSize;

	/**
	 * We use our own map, although it will contain a single item most of the
	 * time, because ThreadLocal causes (huge) memory leaks when used as a
	 * non-static field.
	 * 
	 * @see getLocalFrequentsIterator
	 */
	private static final ThreadLocal<FrequentIterator> localFrequentsIterator = new ThreadLocal<FrequentIterator>() {
		@Override
		protected FrequentIterator initialValue() {
			return new FrequentIterator();
		}
	};

	@Override
	final void eraseItem(int i) {
		this.supportCounts.remove(i);
		this.distinctTransactionsCounts.remove(i);
	}

	@Override
	public boolean compactedRenaming() {
		return this.compactedArrays;
	}

	/**
	 * Does item counting over a projected dataset
	 * 
	 * @param minimumSupport
	 * @param transactions
	 *            extension's support
	 * @param extension
	 *            the item on which we're projecting - it won't appear in *any*
	 *            counter (not even 'closure')
	 * @param ignoredItems
	 *            may be null, if it's not contained items won't appear in any
	 *            counter either
	 * @param maxItem
	 *            biggest index among items to be found in "transactions"
	 * @param reuseReverseRenaming
	 * @param parentPattern
	 */
	public SparseCounters(int minimumSupport, Iterator<TransactionReader> transactions, int extension,
			int[] ignoredItems, final int maxItem, int[] reuseReverseRenaming, int[] parentPattern) {
		// TODO take care of default capacity

		CountersHandler.increment(TopPICounters.NbCounters);
		CountersHandler.increment(TopPICounters.NbSparseCounters);

		this.reverseRenaming = reuseReverseRenaming;
		this.minSupport = minimumSupport;
		this.supportCounts = new TIntIntHashMap(1000, Constants.DEFAULT_LOAD_FACTOR, Integer.MAX_VALUE, 0);
		this.distinctTransactionsCounts = new TIntIntHashMap(1000, Constants.DEFAULT_LOAD_FACTOR, Integer.MAX_VALUE, 0);
		this.rebasingSize = maxItem + 1;
		// item support and transactions counting

		int weightsSum = 0;
		int transactionsCount = 0;

		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			int weight = transaction.getTransactionSupport();

			if (weight > 0) {
				if (transaction.hasNext()) {
					weightsSum += weight;
					transactionsCount++;
				}

				while (transaction.hasNext()) {
					int item = transaction.next();
					if (item <= maxItem) {
						this.supportCounts.adjustOrPutValue(item, weight, weight);
						this.distinctTransactionsCounts.adjustOrPutValue(item, 1, 1);
					}
				}
			}
		}

		this.transactionsCount = weightsSum;
		this.distinctTransactionsCount = transactionsCount;

		// ignored items
		this.eraseItem(extension);
		this.maxCandidate = extension;

		if (ignoredItems != null) {
			for (int item : ignoredItems) {
				if (item <= maxItem) {
					this.eraseItem(item);
				}
			}
		}

		// item filtering and final computations : some are infrequent, some
		// belong to closure

		ItemsetsFactory closureBuilder = new ItemsetsFactory();
		int remainingDistinctTransLengths = 0;
		int remainingFrequents = 0;
		int biggestItemID = 0;
		TIntIntIterator supportIterator = this.supportCounts.iterator();
		while (supportIterator.hasNext()) {
			supportIterator.advance();
			if (supportIterator.value() < minimumSupport) {
				supportIterator.remove();
				this.distinctTransactionsCounts.remove(supportIterator.key());
			} else if (supportIterator.value() == this.transactionsCount) {
				closureBuilder.add(supportIterator.key());
				supportIterator.remove();
				this.distinctTransactionsCounts.remove(supportIterator.key());
			} else {
				biggestItemID = Math.max(biggestItemID, supportIterator.key());
				remainingFrequents++;
				remainingDistinctTransLengths += supportIterator.value();
			}
		}

		this.closure = closureBuilder.get();

		if (parentPattern.length == 0 && extension >= this.reverseRenaming.length) {
			this.pattern = Arrays.copyOf(this.closure, this.closure.length);
			for (int i = 0; i < this.pattern.length; i++) {
				this.pattern[i] = this.reverseRenaming[this.pattern[i]];
			}
		} else {
			this.pattern = ItemsetsFactory.extendRename(this.closure, extension, parentPattern, this.reverseRenaming);
		}

		this.distinctTransactionLengthSum = remainingDistinctTransLengths;
		this.nbFrequents = remainingFrequents;
		this.maxFrequent = biggestItemID;
	}

	private SparseCounters(int minSupport, int transactionsCount, int distinctTransactionsCount,
			int distinctTransactionLengthSum, TIntIntMap supportCounts, TIntIntMap distinctTransactionsCounts,
			int[] closure, int[] pattern, int nbFrequents, int maxFrequent, int[] reverseRenaming,
			boolean compactedArrays, int maxCandidate, int rebasingSize) {
		super();
		this.minSupport = minSupport;
		this.transactionsCount = transactionsCount;
		this.distinctTransactionsCount = distinctTransactionsCount;
		this.distinctTransactionLengthSum = distinctTransactionLengthSum;
		this.supportCounts = supportCounts;
		this.distinctTransactionsCounts = distinctTransactionsCounts;
		this.closure = closure;
		this.pattern = pattern;
		this.nbFrequents = nbFrequents;
		this.maxFrequent = maxFrequent;
		this.reverseRenaming = reverseRenaming;
		this.compactedArrays = compactedArrays;
		this.maxCandidate = maxCandidate;
		this.rebasingSize = rebasingSize;
	}

	@Override
	protected SparseCounters clone() {
		return new SparseCounters(minSupport, transactionsCount, distinctTransactionsCount,
				distinctTransactionLengthSum, new TIntIntHashMap(supportCounts), new TIntIntHashMap(
						distinctTransactionsCounts), Arrays.copyOf(closure, closure.length), Arrays.copyOf(pattern,
						pattern.length), nbFrequents, maxFrequent, Arrays.copyOf(reverseRenaming,
						reverseRenaming.length), compactedArrays, maxCandidate, rebasingSize);
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
	public int[] compressRenaming(int[] olderReverseRenaming) {
		if (olderReverseRenaming == null) {
			olderReverseRenaming = this.reverseRenaming;
		}
		this.rebasedDistinctTransactionsCounts = new int[this.nbFrequents];
		this.rebasedSupportCounts = new int[this.nbFrequents];
		int[] renaming = new int[Math.max(olderReverseRenaming.length, this.rebasingSize)];
		Arrays.fill(renaming, -1);
		this.reverseRenaming = new int[this.nbFrequents];

		// we will always have newItemID <= item
		int newItemID = 0;
		int greatestBelowMaxCandidate = Integer.MIN_VALUE;
		int[] keys = this.supportCounts.keys();
		Arrays.sort(keys);
		for (int key : keys) {
			renaming[key] = newItemID;
			this.reverseRenaming[newItemID] = olderReverseRenaming[key];
			this.rebasedDistinctTransactionsCounts[newItemID] = this.distinctTransactionsCounts.get(key);
			this.rebasedSupportCounts[newItemID] = this.supportCounts.get(key);
			if (key < this.maxCandidate) {
				greatestBelowMaxCandidate = newItemID;
			}
			newItemID++;
		}
		this.supportCounts = null;
		this.distinctTransactionsCounts = null;
		this.maxCandidate = greatestBelowMaxCandidate + 1;
		this.maxFrequent = this.nbFrequents - 1;
		this.compactedArrays = true;
		return renaming;
	}

	private void quickSortOnSup(int start, int end) {
		if (start >= end - 1) {
			// size 0 or 1
			return;
		} else if (end - start == 2) {
			if (this.rebasedSupportCounts[start] < this.rebasedSupportCounts[start + 1]
					|| ((this.rebasedSupportCounts[start] == this.rebasedSupportCounts[start + 1] && this.reverseRenaming[start] > this.reverseRenaming[start + 1]))) {
				this.swap(start, start + 1);
			}
		} else {
			// pick pivot at the middle and put it at the end
			int pivotPos = start + ((end - start) / 2);
			int pivotVal = this.rebasedSupportCounts[pivotPos];
			this.swap(pivotPos, end - 1);
			int insertInf = start;
			int insertSup = end - 2;
			for (int i = start; i <= insertSup;) {
				if (this.rebasedSupportCounts[i] > pivotVal
						|| (this.rebasedSupportCounts[i] == pivotVal && this.reverseRenaming[i] < this.reverseRenaming[end - 1])) {
					insertInf++;
					i++;
				} else {
					this.swap(i, insertSup);
					insertSup--;
				}
			}
			this.swap(end - 1, insertSup + 1);
			quickSortOnSup(start, insertInf);
			quickSortOnSup(insertSup + 2, end);

		}
	}

	private void swap(int i, int j) {
		int temp;
		temp = this.rebasedSupportCounts[i];
		this.rebasedSupportCounts[i] = this.rebasedSupportCounts[j];
		this.rebasedSupportCounts[j] = temp;
		temp = this.reverseRenaming[i];
		this.reverseRenaming[i] = this.reverseRenaming[j];
		this.reverseRenaming[j] = temp;
		temp = this.rebasedDistinctTransactionsCounts[i];
		this.rebasedDistinctTransactionsCounts[i] = this.rebasedDistinctTransactionsCounts[j];
		this.rebasedDistinctTransactionsCounts[j] = temp;
	}

	final public boolean raiseMinimumSupport(PerItemTopKCollector topKcoll, boolean careAboutFutureExtensions) {
		int updatedMinSupport = Integer.MAX_VALUE;
		for (int item : this.pattern) {
			int bound = topKcoll.getBound(item);
			if (bound <= this.transactionsCount) {
				updatedMinSupport = Math.min(updatedMinSupport, bound);
				if (updatedMinSupport <= this.minSupport) {
					return false;
				}
			}
		}
		if (careAboutFutureExtensions) {
			TIntIntIterator supportIterator = this.supportCounts.iterator();
			while (supportIterator.hasNext()) {
				supportIterator.advance();
				if (supportIterator.key() < this.maxCandidate) {
					int bound = topKcoll.getBound(this.reverseRenaming[supportIterator.key()]);
					if (bound <= supportIterator.value()) {
						updatedMinSupport = Math.min(updatedMinSupport, bound);
						if (updatedMinSupport <= this.minSupport) {
							return false;
						}
					}
				}
			}
		}
		int remainingDistinctTransLengths = 0;
		int remainingFrequents = 0;
		int biggestItemID = 0;
		this.minSupport = updatedMinSupport;
		TIntIntIterator supportIterator = this.supportCounts.iterator();
		while (supportIterator.hasNext()) {
			supportIterator.advance();
			if (supportIterator.value() < this.minSupport) {
				supportIterator.remove();
				this.distinctTransactionsCounts.remove(supportIterator.key());
			} else {
				biggestItemID = Math.max(biggestItemID, supportIterator.key());
				remainingFrequents++;
				remainingDistinctTransLengths += supportIterator.value();
			}
		}
		CountersHandler.add(TopPICounters.DatasetReductionByEpsilonRaising, this.distinctTransactionLengthSum
				- remainingDistinctTransLengths);
		this.distinctTransactionLengthSum = remainingDistinctTransLengths;
		this.nbFrequents = remainingFrequents;
		this.maxFrequent = biggestItemID;
		return true;
	}

	final public int insertUnclosedPatterns(PerItemTopKCollector topKcoll, boolean outputPatternsForFutureExtensions) {
		int[] topKDistinctSupports = new int[topKcoll.getK()];
		int[] topKCorrespondingItems = new int[topKcoll.getK()];
		boolean highestUnique = false;
		// split between extension candidates and others ?
		// set a max because some items will never be able to raise their
		// threshold anyway?
		TIntIntIterator supportIterator = this.supportCounts.iterator();
		while (supportIterator.hasNext()) {
			supportIterator.advance();
			if (outputPatternsForFutureExtensions && supportIterator.key() < this.maxCandidate) {
				topKcoll.collectUnclosedForItem(supportIterator.value(), this.pattern,
						this.reverseRenaming[supportIterator.value()]);
			} else {
				highestUnique = updateTopK(topKDistinctSupports, topKCorrespondingItems, supportIterator.key(),
						supportIterator.value(), highestUnique);
			}
		}
		boolean highest = true;
		int lastInsertSupport = this.minSupport;
		for (int i = topKDistinctSupports.length - 1; i >= 0; i--) {
			if (topKDistinctSupports[i] == 0) {
				// AKA I didn't find k distinct supports
				lastInsertSupport = -1;
				break;
			} else {
				int[] newPattern = Arrays.copyOf(this.pattern, this.pattern.length + 1);
				newPattern[pattern.length] = this.reverseRenaming[topKCorrespondingItems[i]];
				topKcoll.collect(topKDistinctSupports[i], newPattern, highest && highestUnique);
				lastInsertSupport = topKDistinctSupports[i];
			}
			highest = false;
		}
		return lastInsertSupport;
	}

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
	public int[] compressSortRenaming(int[] olderReverseRenaming) {
		if (olderReverseRenaming == null) {
			olderReverseRenaming = this.reverseRenaming;
		}
		this.rebasedDistinctTransactionsCounts = new int[this.nbFrequents];
		this.rebasedSupportCounts = new int[this.nbFrequents];
		this.reverseRenaming = new int[this.nbFrequents];
		// first, compact
		// we will always have newItemID <= item
		int newItemIDBelowCandidate = 0;
		int newItemIDAboveCandidate = this.nbFrequents - 1;
		TIntIntIterator supportIterator = this.supportCounts.iterator();
		// after this loop we have
		// reverseRenaming: NewBase (index) -> PreviousDatasetBase (value)
		// supportCounts: NewBase (index) -> Support (value)
		// distinctTransactionCount: NewBase (index) -> Count (value)
		while (supportIterator.hasNext()) {
			supportIterator.advance();
			if (supportIterator.key() < this.maxCandidate) {
				this.reverseRenaming[newItemIDBelowCandidate] = supportIterator.key();
				this.rebasedSupportCounts[newItemIDBelowCandidate] = supportIterator.value();
				this.rebasedDistinctTransactionsCounts[newItemIDBelowCandidate] = this.distinctTransactionsCounts
						.get(supportIterator.key());
				newItemIDBelowCandidate++;
			} else {
				this.reverseRenaming[newItemIDAboveCandidate] = supportIterator.key();
				this.rebasedSupportCounts[newItemIDAboveCandidate] = supportIterator.value();
				this.rebasedDistinctTransactionsCounts[newItemIDAboveCandidate] = this.distinctTransactionsCounts
						.get(supportIterator.key());
				newItemIDAboveCandidate--;
			}
		}
		this.supportCounts = null;
		this.distinctTransactionsCounts = null;
		this.maxCandidate = newItemIDBelowCandidate;
		this.maxFrequent = this.nbFrequents - 1;

		// now, sort up to the pivot
		this.quickSortOnSup(0, this.maxCandidate);

		int[] renaming = new int[Math.max(olderReverseRenaming.length, this.rebasingSize)];
		Arrays.fill(renaming, -1);

		// after this loop we have
		// reverseRenaming: NewBase (index) -> OriginalBase (value)
		// renaming: PreviousDatasetBase (index) -> NewBase (value)

		for (int i = 0; i <= this.maxFrequent; i++) {
			renaming[this.reverseRenaming[i]] = i;
			this.reverseRenaming[i] = olderReverseRenaming[this.reverseRenaming[i]];
		}

		this.compactedArrays = true;
		return renaming;
	}

	/**
	 * Notice: enumerated item IDs are in local base, use this.reverseRenaming
	 * 
	 * @return an iterator over frequent items (in ascending order)
	 */
	public FrequentsIterator getLocalFrequentsIterator(final int from, final int to) {
		FrequentIterator iterator = localFrequentsIterator.get();
		iterator.recycle(from, to, this);
		return iterator;
	}

	public final int getDistinctTransactionsCount(int item) {
		if (compactedArrays) {
			return this.rebasedDistinctTransactionsCounts[item];
		} else {
			return distinctTransactionsCounts.get(item);
		}
	}

	public final int getSupportCount(int item) {
		if (compactedArrays) {
			return this.rebasedSupportCounts[item];
		} else {
			return this.supportCounts.get(item);
		}
	}
}
