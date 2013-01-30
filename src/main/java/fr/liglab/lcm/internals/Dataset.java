package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.map.hash.TIntLongHashMap;

/**
 * Once an actual Dataset class is instanciated, it provides public accessors to 
 * - transactions count
 * - remaining frequent items
 * - discovered items to be added in current pattern's closure
 * 
 * Each dataset is _implicitely_ reduced for a pattern, which may be
 *  - the empty pattern
 *  - its parent's pattern (considering the Dataset provided at instanciation as its parent) + its extension + its discovered closure
 */
public class Dataset {
	/**
	 * Filtered dataset
	 * 
	 * Its transactions only contains items with (global) support in [minsup, 100% [
	 */
  	private Transactions data;
	
	/**
	 * Map frequent items to their support count
	 */
	private TIntLongHashMap frequents = new TIntLongHashMap();
	
	/**
	 * Items found to have a 100% support
	 * These will never appear in "frequents"
	 */
	private Itemset closure = new Itemset();
	
	/**
	 * Constructor for the initial dataset
	 */
	public Dataset(Long minsup, Transactions transactions) {
		data = new Transactions();
		
		for (Itemset transaction : transactions) {
			incrementItemSupports(transaction);
		}
		
		postItemCounting(minsup, (long) transactions.size());
		
		for (Itemset transaction : transactions) {
			Itemset filtered_transaction = new Itemset();
			TIntIterator iterator = transaction.iterator();
			
			while(iterator.hasNext()) {
				int item = iterator.next();
				if (frequents.containsKey(item)) {
					filtered_transaction.add(item);
				}
			}
			
			data.add(filtered_transaction);
		}
	}
	
	/**
	 * Reduce the original dataset by keeping only "extension" occurences
	 * We assume "extension" is a frequent item in the original dataset
	 */
	public Dataset(Long minsup, Dataset original, int extension) {
		data = new Transactions((int) original.frequents.get(extension));
		
		for (int tid = 0; tid < original.data.size(); tid++) {
			Itemset transaction = original.data.get(tid);
			
			if (transaction.contains(extension)) {
				data.add(transaction);
				incrementItemSupports(transaction);
			}
		}
		
		frequents.remove(extension);
		postItemCounting(minsup, (long) data.size());
	}
	
	/**
	 * For each item in the given transaction, increment their support count in frequents
	 */
	private void incrementItemSupports(Itemset transaction) {
		TIntIterator iterator = transaction.iterator();
		while(iterator.hasNext()) {
			int item = iterator.next();
			if (!frequents.increment(item)) {
				frequents.put(item, 1);
			}
		}
	}
	
	/**
	 * Filter unfrequent items and move relevant items to closure by the way
	 */
	private void postItemCounting(Long minsup, Long closuressup) {
		TIntLongIterator iterator = frequents.iterator();
		while (iterator.hasNext()) {
			iterator.advance();
			
			if (iterator.value() < minsup) {
				iterator.remove();
			} else if (iterator.value() == closuressup) {
				closure.add(iterator.key());
				iterator.remove();
			}
		}
	}
	
	/**
	 * May happen if not a single item is above the minimum support,
	 * or if the pattern provided in the constructor is not frequent 
	 */
	public boolean isEmpty() {
		return data.isEmpty();
	}
	
	public Long getTransactionsCount() {
		return (long) data.size();
	}
	
	public Itemset getClosureExtension() {
		return closure;
	}
	
	public int[] getFrequentItems() {
		return frequents.keys();
	}
}
