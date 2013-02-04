package fr.liglab.lcm.internals;

import java.util.Collection;

/**
 * Itemsets and patterns are represented by classic integer arrays
 * This class contains some utility methods
 * 
 * it's abstract so you can't instanciate it
 */
public abstract class Itemsets {
	
	/**
	 * @return a new array concatenating each of its arguments
	 */
	public static int[] extend(int[] pattern, int extension, int[] closure) {
		int[] extended = new int[pattern.length + closure.length + 1];
		
		System.arraycopy(pattern, 0, extended, closure.size(), pattern.length);
		System.arraycopy(pattern, 0, extended, closure.size(), pattern.length);
		
		extended[extended.length - 1] = extension;
		
		return extended;
	}
	
	
	
}
