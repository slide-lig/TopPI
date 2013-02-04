package fr.liglab.lcm.internals;

import java.util.Iterator;

/**
 * a Dataset represents a transactions database
 * 
 * Sub-classes are instanciated explicitely or by applying a projection.
 * At instanciation time, internal (and interesting) information 
 * about contained transactions are gathered and made available by the 
 * following common methods 
 */
public abstract class Dataset {
	
	/**
	 * @return how many transactions are represented by this dataset ?
	 */
	public abstract long getTransactionsCount();
	
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
	 * TODO
	 *  maybe decorate another iterator with Fast Prefix-Preservation Check?
		fpp-checks : no item in ] extension; candidate [ has the same support as candidate
	 * 
	 * @return
	 */
	public abstract Iterator<int> getCandidatesIterator();
}
