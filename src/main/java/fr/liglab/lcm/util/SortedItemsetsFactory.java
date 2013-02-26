package fr.liglab.lcm.util;

import java.util.Arrays;


/**
 * An ItemsetsFactory that sorts (in ascending order) items before returning 
 * the constructed array.
 */
public class SortedItemsetsFactory extends ItemsetsFactory {
	
	/**
	 * Resets the builder by the way
	 * @return a sorted array containing latest items added.
	 */
	@Override
	public int[] get() {
		int[] res = super.get();
		Arrays.sort(res);
		return res;
	}
}
