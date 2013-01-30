package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.list.array.TIntArrayList;
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
	 * Its transactions only contains items with support in [minsup, 100% [
	 */
	private Transactions data = new Transactions();
	
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
		for (Itemset transaction : transactions) {
			incrementItemSupports(transaction);
		}
		
		postItemCounting(minsup, (long) data.size());
		
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
	 * Infrequent extension will yield an empty Dataset
	 */
	public Dataset(Long minsup, Dataset original, int extension) {
		TIntArrayList tids = new TIntArrayList(); // extension's occurences list
		
		for (int tid = 0; tid < original.data.size(); tid++) {
			Itemset transaction = original.data.get(tid);
			
			if (transaction.contains(extension)) {
				tids.add(tid);
				incrementItemSupports(transaction);
			}
		}
		
		if (tids.size() < minsup) {
			return;
		}
		
		frequents.remove(extension);
		postItemCounting(minsup, (long) tids.size());
		
		TIntIterator iterator = tids.iterator();
		while (iterator.hasNext()) {
			int tid = iterator.next();
			
			Itemset filtered_transaction = new Itemset();
			TIntIterator items = original.data.get(tid).iterator();
			
			while(items.hasNext()) {
				int item = items.next();
				if (frequents.containsKey(item)) {
					filtered_transaction.add(item);
				}
			}
			
			data.add(filtered_transaction);
		}
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
