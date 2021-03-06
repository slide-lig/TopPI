/*
	This file is part of TopPI - see https://github.com/slide-lig/TopPI/
	
	Copyright 2016 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia, Marie-Christine Rousset, Université Grenoble Alpes, LIG, CNRS
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	 http://www.apache.org/licenses/LICENSE-2.0
	 
	or see the LICENSE.txt file joined with this program.
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package fr.liglab.mining.internals;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;

import javax.xml.ws.Holder;

import fr.liglab.mining.CountersHandler;
import fr.liglab.mining.CountersHandler.TopPICounters;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.util.ItemAndSupport;
import fr.liglab.mining.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

public class DenseCounters extends Counters {

	/**
	 * Support count, per item having a support count in [minSupport; 100% [
	 * Items having a support count below minSupport are considered infrequent,
	 * those at 100% belong to closure, for both supportCounts[i] = 0 - except
	 * if renaming happened, in which case such items no longer exists.
	 * 
	 * Indexes above maxFrequent should be considered valid.
	 */
	private int[] supportCounts;

	/**
	 * For each item having a support count in [minSupport; 100% [ , gives how
	 * many distinct transactions contained this item. It's like supportCounts
	 * if all transactions have a weight equal to 1
	 * 
	 * Indexes above maxFrequent should be considered valid.
	 */
	private int[] distinctTransactionsCounts;

	/**
	 * will be set to true if arrays have been compacted, ie. if supportCounts
	 * and distinctTransactionsCounts don't contain any zero.
	 */
	private boolean compactedArrays = false;

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
		this.supportCounts[i] = 0;
		this.distinctTransactionsCounts[i] = 0;
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
	public DenseCounters(int minimumSupport, Iterator<TransactionReader> transactions, int extension,
			int[] ignoredItems, final int maxItem, int[] reuseReverseRenaming, int[] parentPattern) {

		CountersHandler.increment(TopPICounters.NbCounters);

		this.reverseRenaming = reuseReverseRenaming;
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
					// need to check this because of views
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
	DenseCounters(int minimumSupport, Iterator<TransactionReader> transactions, Holder<int[]> renamingHolder) {

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
		renamingHolder.value = new int[biggestItemID + 1];
		Arrays.fill(renamingHolder.value, -1);

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

			renamingHolder.value[item] = newItemID;
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

	/**
	 * Applies items over-filtering to another Counters ASSUMES THAT "initial"
	 * HAS BEEN COUNTER OVER AN UNCOMPRESSED DATASET AND THAT ITEMS in "initial"
	 * ARE SORTED BY DECREASING SUPPORT COUNTS that is one where all transaction
	 * weight 1
	 */
	public DenseCounters(DenseCounters initial, int newThreshold) {
		CountersHandler.increment(TopPICounters.NbCounters);

		this.reverseRenaming = initial.reverseRenaming;
		this.closure = initial.closure;
		this.pattern = initial.pattern;
		this.minSupport = newThreshold;

		int maxItem = 0;
		int totalLen = 0;
		int[] sc = initial.supportCounts;
		while (maxItem < sc.length && sc[maxItem] >= newThreshold) {
			totalLen += sc[maxItem];
			maxItem++;
		}
		this.nbFrequents = maxItem;
		this.maxCandidate = maxItem;
		this.maxFrequent = maxItem - 1;
		this.distinctTransactionLengthSum = totalLen;

		this.supportCounts = new int[this.maxCandidate];
		System.arraycopy(initial.supportCounts, 0, this.supportCounts, 0, this.maxCandidate);

		// NOTE: these ones will be outdated once we've constructed the
		// filtered-merged dataset
		// but we don't care, because they're not accessed after this
		// construction...
		this.transactionsCount = initial.transactionsCount;
		this.distinctTransactionsCount = initial.distinctTransactionsCount;
		this.distinctTransactionsCounts = initial.distinctTransactionsCounts;
	}

	private DenseCounters(int minSupport, int transactionsCount, int distinctTransactionsCount,
			int distinctTransactionLengthSum, int[] supportCounts, int[] distinctTransactionsCounts, int[] closure,
			int[] pattern, int nbFrequents, int maxFrequent, int[] reverseRenaming, boolean compactedArrays,
			int maxCandidate) {
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
	}

	@Override
	protected DenseCounters clone() {
		return new DenseCounters(minSupport, transactionsCount, distinctTransactionsCount,
				distinctTransactionLengthSum, Arrays.copyOf(supportCounts, supportCounts.length), Arrays.copyOf(
						distinctTransactionsCounts, distinctTransactionsCounts.length), Arrays.copyOf(closure,
						closure.length), Arrays.copyOf(pattern, pattern.length), nbFrequents, maxFrequent,
				Arrays.copyOf(reverseRenaming, reverseRenaming.length), compactedArrays, maxCandidate);
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
		return renaming;
	}

	private void quickSortOnSup(int start, int end) {
		if (start >= end - 1) {
			// size 0 or 1
			return;
		} else if (end - start == 2) {
			if (this.supportCounts[start] < this.supportCounts[start + 1]
					|| ((this.supportCounts[start] == this.supportCounts[start + 1] && this.reverseRenaming[start] > this.reverseRenaming[start + 1]))) {
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
				if (this.supportCounts[i] > pivotVal
						|| (this.supportCounts[i] == pivotVal && this.reverseRenaming[i] < this.reverseRenaming[end - 1])) {
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

	final public boolean raiseMinimumSupport(PerItemTopKCollector topKcoll) {
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
		for (int i = 0; i < this.maxCandidate; i++) {
			int count = this.getSupportCount(i);
			if (count != 0) {
				int bound = topKcoll.getBound(this.reverseRenaming[i]);
				if (bound <= count) {
					updatedMinSupport = Math.min(updatedMinSupport, bound);
					if (updatedMinSupport <= this.minSupport) {
						return false;
					}
				}
			}
		}
		int remainingDistinctTransLengths = 0;
		int remainingFrequents = 0;
		int biggestItemID = 0;
		this.minSupport = updatedMinSupport;
		for (int i = 0; i <= this.maxFrequent; i++) {
			int count = this.getSupportCount(i);
			if (count != 0) {
				if (count < this.minSupport) {
					this.eraseItem(i);
				} else {
					biggestItemID = Math.max(biggestItemID, i);
					remainingFrequents++;
					remainingDistinctTransLengths += count;
				}
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
		for (int i = 0; i < this.maxCandidate; i++) {
			int count = this.getSupportCount(i);
			if (count != 0) {
				if (outputPatternsForFutureExtensions) {
					topKcoll.collectUnclosedForItem(count, this.pattern, this.reverseRenaming[i]);
				}
				highestUnique = updateTopK(topKDistinctSupports, topKCorrespondingItems, i, count, highestUnique);
			}
		}
		for (int i = this.maxCandidate; i <= this.maxFrequent; i++) {
			int count = this.getSupportCount(i);
			if (count != 0) {
				highestUnique = updateTopK(topKDistinctSupports, topKCorrespondingItems, i, count, highestUnique);
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

		int[] renaming = new int[Math.max(olderReverseRenaming.length, this.supportCounts.length)];
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
		return distinctTransactionsCounts[item];
	}

	public final int getSupportCount(int item) {
		return this.supportCounts[item];
	}

	public final int[] getSupportCounts() {
		return this.supportCounts;
	}
}
