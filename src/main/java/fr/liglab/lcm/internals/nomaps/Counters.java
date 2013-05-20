package fr.liglab.lcm.internals.nomaps;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import fr.liglab.lcm.internals.FrequentsIterator;
import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.util.ItemAndSupport;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * This class' constructor performs item counting over a transactions database, and then gives 
 * access to various counters. It ignores items below the minimum support. During recursions this 
 * allows the algorithm to choose what kind of projections and filtering should be done, before 
 * instantiating the actual projected dataset.
 * 
 * We're using arrays as Maps<Int,Int> , however they're not public as item identifiers may have 
 * been renamed by dataset representation. By the way this class is able to generate renamings 
 * (and applies them to itself by the way) if you need to rename items in a future representation.
 * You *MUST* handle renaming after instantiation. See field reverseRenaming.
 */
public final class Counters {
	
	/**
	 * Items occuring less than minSup times will be considered infrequent
	 */
	public final int minSupport;
	
	/**
	 * How many transactions are represented by the given dataset ?
	 */
	public final int transactionsCount;
	
	/**
	 * How many transactions have been counted (equals transactionsCount when all transactions
	 * have a weight of 1)
	 */
	public final int distinctTransactionsCount;
	
	/**
	 * Sum of given *filtered* transactions' lengths, ignoring their weight
	 */
	public final int distinctTransactionLengthSum;
	
	/**
	 * Support count, per item having a support count in [minSupport; 100% [
	 * Items having a support count below minSupport are considered infrequent, those at 100% belong 
	 * to closure, for both supportCounts[i] = 0 - except if renaming happened, in which case such
	 * items no longer exists.
	 * 
	 * Indexes above maxFrequent should be considered valid.
	 */
	final int[] supportCounts;
	
	/**
	 * supportCounts, summed - FIXME is this useful ??
	 */
	public final int supportCountsSum;
	
	/**
	 * For each item having a support count in [minSupport; 100% [ , gives how many distinct 
	 * transactions contained this item. It's like supportCounts if all transactions have a 
	 * weight equal to 1 
	 * 
	 * Indexes above maxFrequent should be considered valid.
	 */
	final int[] distinctTransactionsCounts;
	
	/**
	 * Items found to have a support count equal to transactionsCount (using IDs from given transactions)
	 * On renamed datasets you SHOULD NOT use getReverseRenaming to translate back these items, 
	 * rather use parent's reverseRenaming (or none for the initial dataset)
	 */
	final int[] closure;
	
	/**
	 * Counts how many items have a support count in [minSupport; 100% [
	 */
	public final int nbFrequents;
	
	/**
	 * Biggest item ID having a support count in [minSupport; 100% [
	 */
	protected int maxFrequent;
	
	/**
	 * This array allows another class to output the discovered closure using original items' IDs.
	 * 
	 * After instanciation this field *must* be set by one of these methods
	 *  - reuseRenaming
	 *  - the initial dataset's constructor (which also sets "renaming")
	 *  - compactRenaming (useful when recompacting dataset in recursions)
	 */
	protected int[] reverseRenaming;
	
	/**
	 * This field will be null EXCEPT if you're using the initial dataset's constructor, in which 
	 * case it computes its absolute renaming by the way.
	 * 
	 * It gives, for each original item ID, its new identifier. If it's negative it means the item 
	 * should be filtered.
	 */
	final int[] renaming;
	
	/**
	 * will be set to true if arrays have been compacted, ie. if supportCounts and distinctTransactionsCounts
	 * don't contain any zero.
	 */
	protected boolean compactedArrays = false;
	
	
	/**
	 * Does item counting over a projected dataset
	 * @param minimumSupport
	 * @param transactions extension's support
	 * @param extension the item on which we're projecting - it won't appear in *any* counter (not even 'closure')
	 * @param ignoredItems may be null, if it's not contained items won't appear in any counter either
	 * @param maxItem biggest index among items to be found in "transactions"
	 */
	Counters(int minimumSupport, Iterator<TransactionReader> transactions, int extension,
			int[] ignoredItems, final int maxItem) {
		
		this.renaming = null;
		this.minSupport = minimumSupport;
		this.supportCounts = new int[maxItem];
		this.distinctTransactionsCounts = new int[maxItem];
		
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
					
					if (item < maxItem) {
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
		
		if (ignoredItems != null) {
			for (int item : ignoredItems) {
				if (item < maxItem) {
					this.supportCounts[item] = 0;
					this.distinctTransactionsCounts[item] = 0;
				}
			}
		}
		
		// item filtering and final computations : some are infrequent, some belong to closure 
		
		ItemsetsFactory closureBuilder = new ItemsetsFactory();
		int remainingSupportsSum = 0;
		int remainingDistinctTransLength = 0;
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
				remainingSupportsSum += this.supportCounts[i];
				remainingDistinctTransLength += this.distinctTransactionsCounts[i];
			}
		}

		this.closure = closureBuilder.get();
		this.supportCountsSum = remainingSupportsSum;
		this.distinctTransactionLengthSum = remainingDistinctTransLength;
		this.nbFrequents = remainingFrequents;
		this.maxFrequent = biggestItemID;
	}
	
	/**
	 * Does item counting over an initial dataset : it will only ignore infrequent items, and it 
	 * doesn't know what's biggest item ID. 
	 * IT ALSO IGNORES TRANSACTIONS WEIGHTS ! (assuming it's 1 everywhere)
	 * /!\ It will perform an absolute renaming : items are renamed (and, likely, re-ordered) by 
	 * decreasing support count. For instance 0 will be the most frequent item.
	 * 
	 * Indexes in arrays will refer items' new names, except for closure.
	 * 
	 * @param minimumSupport
	 * @param transactions
	 */
	Counters(int minimumSupport, Iterator<TransactionReader> transactions) {
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
		this.renaming = new int[biggestItemID];
		Arrays.fill(renaming, -1);
		
		// item filtering and final computations : some are infrequent, some belong to closure 

		final PriorityQueue<ItemAndSupport> renamingHeap = new PriorityQueue<ItemAndSupport>();
		ItemsetsFactory closureBuilder = new ItemsetsFactory();
		
		TIntIntIterator iterator = supportsMap.iterator();
		
		while (iterator.hasNext()) {
			final int item = iterator.key();
			final int supportCount = iterator.value();
			
			if (supportCount == this.transactionsCount) {
				closureBuilder.add(item);
			} else if (supportCount >= minimumSupport){
				renamingHeap.add(new ItemAndSupport(item, supportCount));
			} // otherwise item is infrequent : its renaming is already -1, ciao
		}

		this.closure = closureBuilder.get();
		this.nbFrequents = renamingHeap.size();
		this.maxFrequent = this.nbFrequents - 1;
		
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
		this.supportCountsSum = remainingSupportsSum;
		this.distinctTransactionLengthSum = remainingSupportsSum;
	}
	
	/**
	 * @return greatest frequent item's ID, which is also the greatest valid index for arrays supportCounts and distinctTransactionsCounts
	 */
	int getMaxFrequent() {
		return this.maxFrequent;
	}
	
	/**
	 * @return a translation from 
	 */
	int[] getReverseRenaming() {
		return this.reverseRenaming;
	}
	
	void reuseRenaming(int[] olderReverseRenaming) {
		this.reverseRenaming = olderReverseRenaming;
	}
	
	/**
	 * Will compress an older renaming, by removing infrequent items.
	 * Contained arrays (except closure) will refer new item IDs
	 * @param olderReverseRenaming reverseRenaming from the dataset that fed this Counter
	 * @return the translation from the old renaming to the compressed one (gives -1 for removed items)
	 */
	int[] compressRenaming(int[] olderReverseRenaming) {
		int[] renaming = new int[olderReverseRenaming.length];
		this.reverseRenaming = new int[this.nbFrequents];
		
		// we will always have newItemID <= item
		int newItemID = 0;
		
		for (int item = 0; item < this.supportCounts.length; item++) {
			if (this.supportCounts[item] > 0) {
				renaming[item] = newItemID;
				this.reverseRenaming[newItemID] = olderReverseRenaming[item];
				
				this.distinctTransactionsCounts[newItemID] = this.distinctTransactionsCounts[item];
				this.supportCounts[newItemID] = this.supportCounts[item];
				
				newItemID++;
			} else {
				renaming[item] = -1;
			}
		}
		
		for (int item = this.supportCounts.length; item < olderReverseRenaming.length; item++) {
			renaming[item] = -1;
		}
		
		this.maxFrequent = newItemID - 1;
		this.compactedArrays = true;
		
		return renaming;
	}
	
	FrequentsIterator getFrequentsIterator(final int to) {
		return new AllFrequentsIterator(to);
	}
	
	
	/**
	 * Thread-safe iterator over frequent items (ie. those having a support count in [minSup, 100%[)
	 */
	protected class AllFrequentsIterator implements FrequentsIterator {
		private final AtomicInteger index;
		private final int max;
		
		/**
		 * will provide an iterator on frequent items (in increasing order) in [0,to[
		 */
		public AllFrequentsIterator(final int to) {
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
	}
}
