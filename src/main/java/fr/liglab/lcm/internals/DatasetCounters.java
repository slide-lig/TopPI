package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Item counting and all other stats available from a single pass
 * 
 * Once constructed, all of these are available in public fields
 */
public class DatasetCounters {
	
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
	
	/**
	 * Support count for each item having a support count in [minSup; 100% [
	 */
	public final TIntIntMap supportCounts;
	
	/**
	 * Items found to have a 100% support count
	 */
	public final int[] closure;
	
	
	DatasetCounters(int minimumSupport, Iterator<TransactionReader> transactions) {
		this(minimumSupport, transactions, null);
	}
	
	/**
	 * Counts items appearing in provided transactions
	 * @param minimumSupport
	 * @param transactions
	 * @param ignoreItems (may be null) items that may appear in transactions but should to be counted 
	 */
	DatasetCounters(int minimumSupport, Iterator<TransactionReader> transactions, int[] ignoredItems) {
		this.minSup = minimumSupport;
		
		int lineCount = 0;
		int itemCount = 0;
		this.supportCounts = new TIntIntHashMap();
		
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			int weight = transaction.getTransactionSupport();
			lineCount++;
			
			while (transaction.hasNext()) {
				itemCount++;
				int item = transaction.next();
				supportCounts.adjustOrPutValue(item, weight, weight);
			}
		}
		
		this.transactionsCount = lineCount;
		this.itemsRead = itemCount;
		
		if (ignoredItems != null) {
			for (int item : ignoredItems) {
				supportCounts.remove(item);
			}
		}
		
		ItemsetsFactory builder = new ItemsetsFactory();
		int remainingSupportsSum = 0;
		
		for (TIntIntIterator count = supportCounts.iterator(); count.hasNext();){
			count.advance();
			
			if (count.value() < minimumSupport) {
				count.remove();
			} else if (count.value() == this.transactionsCount) {
				builder.add(count.key());
				count.remove();
			} else {
				remainingSupportsSum += count.value();
			}
		}
		
		this.closure = builder.get();
		this.supportsSum = remainingSupportsSum;
	}
	
	/**
	 * @return items having a support count in [minSup; 100% [ (sorted in ascending)
	 */
	public final int[] getSortedFrequents() {
		int[] frequents = this.supportCounts.keys();
		Arrays.sort(frequents);
		return frequents;
	}
	
	/**
	 * @return item having a support count in [minSup; 100% [
	 */
	public final TIntSet getFrequents() {
		return this.supportCounts.keySet();
	}
}
