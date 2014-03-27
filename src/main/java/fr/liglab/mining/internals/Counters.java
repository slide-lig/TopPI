package fr.liglab.mining.internals;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import fr.liglab.mining.CountersHandler;
import fr.liglab.mining.CountersHandler.TopLCMCounters;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.util.ItemAndSupport;
import fr.liglab.mining.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

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
public final class Counters implements Cloneable {

	/**
	 * Items occuring less than minSup times will be considered infrequent
	 */
	public int minSupport;

	/**
	 * How many transactions are represented by the given dataset ?
	 */
	public final int transactionsCount;

	/**
	 * How many transactions have been counted (equals transactionsCount when
	 * all transactions have a weight of 1)
	 */
	public final int distinctTransactionsCount;

	/**
	 * Sum of given *filtered* transactions' lengths, ignoring their weight
	 */
	public int distinctTransactionLengthSum;

	/**
	 * Support count, per item having a support count in [minSupport; 100% [
	 * Items having a support count below minSupport are considered infrequent,
	 * those at 100% belong to closure, for both supportCounts[i] = 0 - except
	 * if renaming happened, in which case such items no longer exists.
	 * 
	 * Indexes above maxFrequent should be considered valid.
	 * 
	 * TODO private+accessor
	 */
	public int[] supportCounts;

	/**
	 * For each item having a support count in [minSupport; 100% [ , gives how
	 * many distinct transactions contained this item. It's like supportCounts
	 * if all transactions have a weight equal to 1
	 * 
	 * Indexes above maxFrequent should be considered valid.
	 * 
	 * TODO private+accessor
	 */
	public int[] distinctTransactionsCounts;

	/**
	 * Items found to have a support count equal to transactionsCount (using IDs
	 * from given transactions) On renamed datasets you SHOULD NOT use
	 * getReverseRenaming to translate back these items, rather use parent's
	 * reverseRenaming (or none for the initial dataset)
	 */
	final int[] closure;

	/**
	 * closure of parent's pattern UNION extension - using original item IDs
	 */
	public final int[] pattern;

	/**
	 * Counts how many items have a support count in [minSupport; 100% [ TODO
	 * protected+accessor
	 */
	public int nbFrequents;

	/**
	 * Biggest item ID having a support count in [minSupport; 100% [
	 */
	protected int maxFrequent;

	/**
	 * This array allows another class to output the discovered closure using
	 * original items' IDs.
	 * 
	 * After instanciation this field *must* be set by one of these methods -
	 * reuseRenaming, the initial dataset's constructor (which also sets
	 * "renaming") - compressRenaming, useful when recompacting dataset in
	 * recursions
	 */
	protected int[] reverseRenaming;

	/**
	 * This field will be null EXCEPT if you're using the initial dataset's
	 * constructor (in which case it computes its absolute renaming by the way)
	 * OR if you called compressRenaming (in which case getRenaming will give
	 * back the same value)
	 * 
	 * It gives, for each original item ID, its new identifier. If it's negative
	 * it means the item should be filtered.
	 */
	protected int[] renaming = null;

	/**
	 * will be set to true if arrays have been compacted, ie. if supportCounts
	 * and distinctTransactionsCounts don't contain any zero.
	 */
	protected boolean compactedArrays = false;

	/**
	 * Exclusive index of the first item >= core_item in current base
	 */
	protected int maxCandidate;

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
	public Counters(int minimumSupport, Iterator<TransactionReader> transactions, int extension, int[] ignoredItems,
			final int maxItem, int[] reuseReverseRenaming, int[] parentPattern) {

		CountersHandler.increment(TopLCMCounters.NbCounters);

		this.reverseRenaming = reuseReverseRenaming;
		this.renaming = null;
		this.minSupport = minimumSupport;
		this.supportCounts = new int[maxItem + 1];
		this.distinctTransactionsCounts = new int[maxItem + 1];

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
						this.supportCounts[item] += weight;
						this.distinctTransactionsCounts[item]++;
					}
				}
			}
		}

		this.transactionsCount = weightsSum;
		this.distinctTransactionsCount = transactionsCount;

		// ignored items
		this.supportCounts[extension] = 0;
		this.distinctTransactionsCounts[extension] = 0;
		this.maxCandidate = extension;

		if (ignoredItems != null) {
			for (int item : ignoredItems) {
				if (item <= maxItem) {
					this.supportCounts[item] = 0;
					this.distinctTransactionsCounts[item] = 0;
				}
			}
		}

		// item filtering and final computations : some are infrequent, some
		// belong to closure

		ItemsetsFactory closureBuilder = new ItemsetsFactory();
		int remainingDistinctTransLengths = 0;
		int remainingFrequents = 0;
		int biggestItemID = 0;

		for (int i = 0; i < this.supportCounts.length; i++) {
			if (this.supportCounts[i] < minimumSupport) {
				this.supportCounts[i] = 0;
				this.distinctTransactionsCounts[i] = 0;
			} else if (this.supportCounts[i] == this.transactionsCount) {
				closureBuilder.add(i);
				this.supportCounts[i] = 0;
				this.distinctTransactionsCounts[i] = 0;
			} else {
				biggestItemID = Math.max(biggestItemID, i);
				remainingFrequents++;
				remainingDistinctTransLengths += this.distinctTransactionsCounts[i];
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

	/*
	 * careAboutFutureExtensions true for toplcm, false for baseline
	 */
	public boolean raiseMinimumSupport(PerItemTopKCollector topKcoll, boolean careAboutFutureExtensions) {
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
			for (int i = 0; i < this.maxCandidate; i++) {
				if (this.supportCounts[i] != 0) {
					int bound = topKcoll.getBound(this.reverseRenaming[i]);
					if (bound <= this.supportCounts[i]) {
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
		for (int i = 0; i < this.supportCounts.length; i++) {
			if (this.supportCounts[i] != 0) {
				if (this.supportCounts[i] < this.minSupport) {
					this.supportCounts[i] = 0;
					this.distinctTransactionsCounts[i] = 0;
				} else {
					biggestItemID = Math.max(biggestItemID, i);
					remainingFrequents++;
					remainingDistinctTransLengths += this.distinctTransactionsCounts[i];
				}
			}
		}
		CountersHandler.add(TopLCMCounters.DatasetReductionByEpsilonRaising, this.distinctTransactionLengthSum
				- remainingDistinctTransLengths);
		this.distinctTransactionLengthSum = remainingDistinctTransLengths;
		this.nbFrequents = remainingFrequents;
		this.maxFrequent = biggestItemID;
		return true;
	}

	public int insertUnclosedPatterns(PerItemTopKCollector topKcoll, boolean outputPatternsForFutureExtensions) {
		int[] topKDistinctSupports = new int[topKcoll.getK()];
		int[] topKCorrespondingItems = new int[topKcoll.getK()];
		boolean highestUnique = false;
		// split between extension candidates and others ?
		// set a max because some items will never be able to raise their
		// threshold anyway?
		for (int i = 0; i < this.maxCandidate; i++) {
			if (this.supportCounts[i] != 0) {
				if (outputPatternsForFutureExtensions) {
					topKcoll.collectUnclosedForItem(this.supportCounts[i], this.pattern, this.reverseRenaming[i]);
				}
				highestUnique = updateTopK(topKDistinctSupports, topKCorrespondingItems, i, this.supportCounts[i],
						highestUnique);
			}
		}
		for (int i = this.maxCandidate; i < this.supportCounts.length; i++) {
			if (this.supportCounts[i] != 0) {
				highestUnique = updateTopK(topKDistinctSupports, topKCorrespondingItems, i, this.supportCounts[i],
						highestUnique);
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

	// true if the current highest in topk can be considered unique
	private static boolean updateTopK(int[] supports, int[] items, int item, int support, boolean highestUnique) {
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
	 * Does item counting over an initial dataset : it will only ignore
	 * infrequent items, and it doesn't know what's biggest item ID. IT ALSO
	 * IGNORES TRANSACTIONS WEIGHTS ! (assuming it's 1 everywhere) /!\ It will
	 * perform an absolute renaming : items are renamed (and, likely,
	 * re-ordered) by decreasing support count. For instance 0 will be the most
	 * frequent item.
	 * 
	 * Indexes in arrays will refer items' new names, except for closure.
	 * 
	 * @param minimumSupport
	 * @param transactions
	 */
	Counters(int minimumSupport, Iterator<TransactionReader> transactions) {
		// no nbCounters because this one is not in a PLCMThread

		this.minSupport = minimumSupport;

		TIntIntHashMap supportsMap = new TIntIntHashMap();
		int biggestItemID = 0;

		// item support and transactions counting

		int transactionsCounter = 0;
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			transactionsCounter++;

			while (transaction.hasNext()) {
				int item = transaction.next();
				biggestItemID = Math.max(biggestItemID, item);
				supportsMap.adjustOrPutValue(item, 1, 1);
			}
		}

		this.transactionsCount = transactionsCounter;
		this.distinctTransactionsCount = transactionsCounter;
		this.renaming = new int[biggestItemID + 1];
		Arrays.fill(renaming, -1);

		// item filtering and final computations : some are infrequent, some
		// belong to closure

		final PriorityQueue<ItemAndSupport> renamingHeap = new PriorityQueue<ItemAndSupport>();
		ItemsetsFactory closureBuilder = new ItemsetsFactory();

		TIntIntIterator iterator = supportsMap.iterator();

		while (iterator.hasNext()) {
			iterator.advance();
			final int item = iterator.key();
			final int supportCount = iterator.value();

			if (supportCount == this.transactionsCount) {
				closureBuilder.add(item);
			} else if (supportCount >= minimumSupport) {
				renamingHeap.add(new ItemAndSupport(item, supportCount));
			} // otherwise item is infrequent : its renaming is already -1, ciao
		}

		this.closure = closureBuilder.get();
		this.pattern = this.closure;
		this.nbFrequents = renamingHeap.size();
		this.maxFrequent = this.nbFrequents - 1;
		this.maxCandidate = this.maxFrequent + 1;

		this.supportCounts = new int[this.nbFrequents];
		this.distinctTransactionsCounts = new int[this.nbFrequents];
		this.reverseRenaming = new int[this.nbFrequents];
		int remainingSupportsSum = 0;

		ItemAndSupport entry = renamingHeap.poll();
		int newItemID = 0;

		while (entry != null) {
			final int item = entry.item;
			final int support = entry.support;

			this.renaming[item] = newItemID;
			this.reverseRenaming[newItemID] = item;

			this.supportCounts[newItemID] = support;
			this.distinctTransactionsCounts[newItemID] = support;

			remainingSupportsSum += support;

			entry = renamingHeap.poll();
			newItemID++;
		}

		this.compactedArrays = true;
		this.distinctTransactionLengthSum = remainingSupportsSum;
	}

	private Counters(int minSupport, int transactionsCount, int distinctTransactionsCount,
			int distinctTransactionLengthSum, int[] supportCounts, int[] distinctTransactionsCounts, int[] closure,
			int[] pattern, int nbFrequents, int maxFrequent, int[] reverseRenaming, int[] renaming,
			boolean compactedArrays, int maxCandidate) {
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
		this.renaming = renaming;
		this.compactedArrays = compactedArrays;
		this.maxCandidate = maxCandidate;
	}

	@Override
	protected Counters clone() {
		return new Counters(minSupport, transactionsCount, distinctTransactionsCount, distinctTransactionLengthSum,
				Arrays.copyOf(supportCounts, supportCounts.length), Arrays.copyOf(distinctTransactionsCounts,
						distinctTransactionsCounts.length), Arrays.copyOf(closure, closure.length), Arrays.copyOf(
						pattern, pattern.length), nbFrequents, maxFrequent, Arrays.copyOf(reverseRenaming,
						reverseRenaming.length), Arrays.copyOf(renaming, renaming.length), compactedArrays,
				maxCandidate);
	}

	/**
	 * @return greatest frequent item's ID, which is also the greatest valid
	 *         index for arrays supportCounts and distinctTransactionsCounts
	 */
	public int getMaxFrequent() {
		return this.maxFrequent;
	}

	/**
	 * @return the renaming map from instantiation's base to current base
	 */
	public int[] getRenaming() {
		return renaming;
	}

	/**
	 * @return a translation from internal item indexes to dataset's original
	 *         indexes
	 */
	public int[] getReverseRenaming() {
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
	public int[] compressRenaming(int[] olderReverseRenaming) {
		if (olderReverseRenaming == null) {
			olderReverseRenaming = this.reverseRenaming;
		}

		int[] renaming = new int[Math.max(olderReverseRenaming.length, this.supportCounts.length)];
		this.reverseRenaming = new int[this.nbFrequents];

		// we will always have newItemID <= item
		int newItemID = 0;
		int greatestBelowMaxCandidate = Integer.MIN_VALUE;

		for (int item = 0; item < this.supportCounts.length; item++) {
			if (this.supportCounts[item] > 0) {
				renaming[item] = newItemID;
				this.reverseRenaming[newItemID] = olderReverseRenaming[item];

				this.distinctTransactionsCounts[newItemID] = this.distinctTransactionsCounts[item];
				this.supportCounts[newItemID] = this.supportCounts[item];

				if (item < this.maxCandidate) {
					greatestBelowMaxCandidate = newItemID;
				}

				newItemID++;
			} else {
				renaming[item] = -1;
			}
		}

		this.maxCandidate = greatestBelowMaxCandidate + 1;
		Arrays.fill(renaming, this.supportCounts.length, renaming.length, -1);

		this.maxFrequent = newItemID - 1;
		this.compactedArrays = true;

		this.renaming = renaming;

		return renaming;
	}

	private void quickSortOnSup(int start, int end) {
		if (start >= end - 1) {
			// size 0 or 1
			return;
		} else if (end - start == 2) {
			if (this.supportCounts[start] < this.supportCounts[start + 1]) {
				this.swap(start, start + 1);
			}
		} else {
			// pick pivot at the middle and put it at the end
			int pivotPos = start + ((end - start) / 2);
			int pivotVal = this.supportCounts[pivotPos];
			this.swap(pivotPos, end - 1);
			int insertInf = start;
			int insertSup = end - 2;
			for (int i = start; i <= insertSup;) {
				if (this.supportCounts[i] > pivotVal || this.supportCounts[i] == pivotVal && i < pivotPos) {
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
		temp = this.supportCounts[i];
		this.supportCounts[i] = this.supportCounts[j];
		this.supportCounts[j] = temp;
		temp = this.reverseRenaming[i];
		this.reverseRenaming[i] = this.reverseRenaming[j];
		this.reverseRenaming[j] = temp;
		temp = this.distinctTransactionsCounts[i];
		this.distinctTransactionsCounts[i] = this.distinctTransactionsCounts[j];
		this.distinctTransactionsCounts[j] = temp;
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

		this.reverseRenaming = new int[this.nbFrequents];
		// first, compact
		// we will always have newItemID <= item
		int newItemID = 0;
		int greatestBelowMaxCandidate = Integer.MIN_VALUE;

		// after this loop we have
		// reverseRenaming: NewBase (index) -> PreviousDatasetBase (value)
		// supportCounts: NewBase (index) -> Support (value)
		// distinctTransactionCount: NewBase (index) -> Count (value)
		for (int item = 0; item < this.supportCounts.length; item++) {
			if (this.supportCounts[item] > 0) {
				this.reverseRenaming[newItemID] = item;
				this.supportCounts[newItemID] = this.supportCounts[item];
				this.distinctTransactionsCounts[newItemID] = this.distinctTransactionsCounts[item];
				if (item < this.maxCandidate) {
					greatestBelowMaxCandidate = newItemID;
				}
				newItemID++;
			}
		}
		this.maxCandidate = greatestBelowMaxCandidate + 1;
		this.maxFrequent = newItemID - 1;

		// now, sort up to the pivot
		this.quickSortOnSup(0, this.maxCandidate);

		this.renaming = new int[Math.max(olderReverseRenaming.length, this.supportCounts.length)];
		Arrays.fill(renaming, -1);

		// after this loop we have
		// reverseRenaming: NewBase (index) -> OriginalBase (value)
		// renaming: PreviousDatasetBase (index) -> NewBase (value)

		for (int i = 0; i <= this.maxFrequent; i++) {
			this.renaming[this.reverseRenaming[i]] = i;
			this.reverseRenaming[i] = olderReverseRenaming[this.reverseRenaming[i]];
		}

		this.compactedArrays = true;
		return renaming;
	}

	public int getMaxCandidate() {
		return maxCandidate;
	}

	/**
	 * Notice: enumerated item IDs are in local base, use this.reverseRenaming
	 * 
	 * @return a thread-safe iterator over frequent items (in ascending order)
	 */
	public FrequentsIterator getExtensionsIterator() {
		return new ExtensionsIterator(this.maxCandidate);
	}

	public FrequentsIterator getReversedExtensionsIterator() {
		return new ReversedExtensionsIterator(this.maxCandidate);
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
			if (compactedArrays) {
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
						if (supportCounts[nextIndex] > 0) {
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

	protected class ReversedExtensionsIterator implements FrequentsIterator {
		private final AtomicInteger index;
		private final int max;

		/**
		 * will provide an iterator on frequent items (in increasing order) in
		 * [0,to[
		 */
		public ReversedExtensionsIterator(final int to) {
			this.index = new AtomicInteger(to - 1);
			this.max = to;
		}

		/**
		 * @return -1 if iterator is finished
		 */
		public int next() {
			if (compactedArrays) {
				final int nextIndex = this.index.getAndDecrement();
				if (nextIndex >= 0) {
					return nextIndex;
				} else {
					return -1;
				}
			} else {
				while (true) {
					final int nextIndex = this.index.getAndDecrement();
					if (nextIndex >= 0) {
						if (supportCounts[nextIndex] > 0) {
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
			return this.max - this.index.get();
		}

		@Override
		public int last() {
			return this.max;
		}
	}

	static protected class FrequentIterator implements FrequentsIterator {

		private int index;
		private int max;
		private int[] supportsFilter;

		FrequentIterator() {
			this.max = 0;
			this.index = 0;
		}

		public void recycle(final int from, final int to, final Counters instance) {
			this.max = to;
			this.index = from;
			this.supportsFilter = instance.compactedArrays ? null : instance.supportCounts;
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
						if (this.supportsFilter[nextIndex] > 0) {
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
