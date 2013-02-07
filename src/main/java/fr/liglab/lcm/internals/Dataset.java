package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;

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
}
