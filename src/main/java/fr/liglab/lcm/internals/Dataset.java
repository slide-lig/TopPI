package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Iterator;

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
	
	/// YES these two are package-visible. See Rebaser.
	
	TIntIntMap supportCounts;
	
	int[] discoveredClosure;
	
	/**
	 * Items in the array are unique and may be unordered
	 * @return items found to have a 100% support count
	 */
	public int[] getDiscoveredClosureItems() {
		return discoveredClosure;
	}
	
	/**
	 * This method could also have been named "getClosureSupportCount"
	 * @return how many transactions are represented by this dataset
	 */
	public abstract int getTransactionsCount();
	
	
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
	public abstract ExtensionsIterator getCandidatesIterator();
	
	
	
	/**
	 * Find (in supportCounts) current dataset's closure (ie. items with 100% support)
	 * By the way, unfrequent items (having a value < minsup) are removed from supportCounts
	 * @return sum of remaining items' support counts
	 */
	protected int genClosureAndFilterCount() {
		ItemsetsFactory builder = new ItemsetsFactory();
		int closureSupport = getTransactionsCount();
		int remainingsSupportsSum = 0;
		
		for (TIntIntIterator count = supportCounts.iterator(); count.hasNext();){
			count.advance();
			
			if (count.value() < minsup) {
				count.remove();
			} else if (count.value() == closureSupport) {
				builder.add(count.key());
				count.remove();
			} else {
				remainingsSupportsSum += count.value();
			}
		}
		
		discoveredClosure = builder.get();
		
		return remainingsSupportsSum;
	}
	
	/**
	 * sets supportCounts, a map [item => support count]
	 */
	protected void genSupportCounts(final Iterator<int[]> transactions) {
		supportCounts = new TIntIntHashMap();
		
		while (transactions.hasNext()) {
			for (int item : transactions.next()) {
				supportCounts.adjustOrPutValue(item, 1, 1);
			}
		}
	}
}
