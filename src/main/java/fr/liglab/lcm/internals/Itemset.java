package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Dummy inheritance will ease future changes of itemsets' actual representation
 */
public class Itemset extends TIntHashSet {
	
	public Itemset() {
		super();
	}
	
	/**
	 * Copy-and-augment constructor
	 */
	public Itemset(Itemset copy, int appendItem) {
		super(copy);
		this.add(appendItem);
	}
	
	/**
	 * @return itemset's biggest item or Integer.MAX_VALUE if it's empty
	 */
	public int max() {
		if (isEmpty()) {
			return Integer.MAX_VALUE;
		} else {
			int max = 0;
			TIntIterator iterator = iterator();
			while (iterator.hasNext()) {
				int item = iterator.next();
				if (item > max) {
					max = item;
				}
			}
			return max;
		}
	}
	
}
