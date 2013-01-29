package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntLongHashMap;

/**
 * This class is intended to wrap (reduced) datasets
 * 
 * At load time it will try to reduce the dataset and extract some 
 * interesting data by the way
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
	 * reduce the original dataset by :
	 * - keeping only "extension" occurences
	 * - removing items having a support outside [minsup, 100% [
	 * 
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
	
	public Long transactionsCount() {
		return (long) data.size();
	}
	
	public Itemset getClosureExtension() {
		return closure;
	}
	
	public int[] getFrequentItems() {
		return frequents.keys();
	}
}
