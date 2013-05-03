package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

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
	
	/**
	 * items having a support count in [minSup; 100% [, sorted
	 */
	public final int[] sortedFrequents;
	
	
	/**
	 * @return item having a support count in [minSup; 100% [
	 */
	public final TIntSet getFrequents() {
		return this.supportCounts.keySet();
	}
	
	/**
	 * @return an iterator on frequent items in ascending order, up to "max" (excluded)
	 */
	public final FrequentsIterator getFrequentsIteratorTo(int max) {
		return new AllFrequentsIterator(max);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		
		sb.append("minSup:");
		sb.append(minSup);
		sb.append(',');

		sb.append("transactionsCount:");
		sb.append(transactionsCount);
		sb.append(',');
		
		sb.append("itemsRead:");
		sb.append(itemsRead);
		sb.append(',');

		sb.append("supportsSum:");
		sb.append(supportsSum);
		sb.append(',');

		sb.append("closure:");
		sb.append(Arrays.toString(closure));
		sb.append(',');

		sb.append("nbFrequents:");
		sb.append(sortedFrequents.length);
		
		sb.append('}');
		return sb.toString();
	}

	/**
	 * @return an iterator on frequent items (in ascending order)
	 */
	public final FrequentsIterator getFrequentsIterator() {
		return new AllFrequentsIterator(Integer.MAX_VALUE);
	}

	DatasetCounters(int minimumSupport, Iterator<TransactionReader> transactions) {
		this(minimumSupport, transactions, Integer.MIN_VALUE, null);
	}
	
	/**
	 * Counts items appearing in provided transactions
	 * @param minimumSupport
	 * @param transactions
	 * @param ingoreItem avoids building an array when there's only extension to be ignored
	 * @param ignoreItems (may be null) items that may appear in transactions but should to be counted 
	 */
	DatasetCounters(int minimumSupport, Iterator<TransactionReader> transactions, 
			int ingoredItem, int[] ignoredItems) {
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
		
		supportCounts.remove(ingoredItem);
		
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
		
		this.sortedFrequents = this.supportCounts.keys();
		Arrays.sort(this.sortedFrequents);
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
		public AllFrequentsIterator(int to) {
			this.index = new AtomicInteger(0);
			
			int toIndex = Arrays.binarySearch(sortedFrequents, to);
			if (toIndex < 0) {
				// binarySearch returns -(insertion_point)-1
				// where insertion_point == index of first element greater OR
				// a.length
				this.max = -toIndex - 1;
			} else {
				this.max = toIndex;
			}
		}
		
		/**
		 * @return -1 if iterator is finished
		 */
		public int next() {
			final int nextIndex = this.index.getAndIncrement();
			if (nextIndex < this.max) {
				return sortedFrequents[nextIndex];
			} else {
				return -1;
			}
		}
	}
}
