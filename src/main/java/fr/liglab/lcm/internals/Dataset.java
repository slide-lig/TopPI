package fr.liglab.lcm.internals;

import java.util.Iterator;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * a Dataset represents a "smart" transactions database : the object 
 * knows the required minimal support count and will gather some 
 * statistics and indexes at instantation time, to ease future lattice 
 * exploration.
 * 
 * Sub-classes are instanciated explicitely or by applying a projection.
 */
public abstract class Dataset {
	
	protected int minsup;
	
	public int getMinsup() {
		return minsup;
	}
	
	/**
	 * This method could also have been named "getClosureSupportCount"
	 * @return how many transactions are represented by this dataset
	 */
	public abstract int getTransactionsCount();
	
	/**
	 * Items in the array are unique and may be unordered
	 * @return items found to have a 100% support count
	 */
	public abstract int[] getDiscoveredClosureItems();
	
	/**
	 * Projects current dataset on the given item, implicitely extending current pattern
	 * 
	 * Let P be a pattern. If this was P's projection, let 
	 * Q = P U {i}
	 * from UnoAUA04-Lemma 3 : "extension" is Q's core index
	 * 
	 * the projected dataset won't forget that !
	 */
	public abstract Dataset getProjection(int extension);
	
	/**
	 * @return candidates items known to be frequent, preserving prefix and not in closure's extension
	 */
	public abstract TIntIterator getCandidatesIterator();
	
	
	
	/**
	 * Find (in supportCounts) current dataset's closure (ie. items with 100% support)
	 * By the way, unfrequent items (having a value < minsup) are removed from supportCounts
	 */
	public int[] getClosureAndFilterCount(TIntIntMap supportCounts) {
		ItemsetsFactory builder = new ItemsetsFactory();
		int closureSupport = getTransactionsCount();
		
		for (TIntIntIterator count = supportCounts.iterator(); count.hasNext();){
			count.advance();
			
			if (count.value() < minsup) {
				count.remove();
			} else if (count.value() == closureSupport) {
				builder.add(count.key());
				count.remove();
			}
		}
				
		return builder.get();
	}
	
	/**
	 * @return support counts for items in given transactions, as a map [item => support count]
	 */
	public static TIntIntMap genSupportCounts(final Iterator<int[]> transactions) {
		TIntIntMap supportCounts = new TIntIntHashMap();
		
		while (transactions.hasNext()) {
			for (int item : transactions.next()) {
				supportCounts.adjustOrPutValue(item, 1, 1);
			}
		}
		
		return supportCounts;
	}
}
