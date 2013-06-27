package fr.liglab.mining.util;

/**
 * A < B <=> A.support > B.support || A.item < B.item
 */
public class ItemAndBigSupport implements Comparable<ItemAndBigSupport> {
	
	public int item;
	public long support;
	
	public ItemAndBigSupport(int i, long s) {
		this.item = i;
		this.support = s;
	}

	/**
	 *  Returns a negative integer, zero, or a positive integer 
	 *  as this object's support is less than, equal to, or 
	 *  greater than the specified object's support. 
	 */
	public int compareTo(ItemAndBigSupport other) {
		if (other.support == this.support) {
			return this.item - other.item;
		} else {
			return (int) (other.support - this.support);
		}
	}
}
