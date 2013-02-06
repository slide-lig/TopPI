package fr.liglab.lcm.internals;

import gnu.trove.list.array.TIntArrayList;

/**
 * Itemsets and patterns are represented by classic integer arrays
 * Aside static utility methods, instanciate it to create arrays without knowing their length beforehand
 */
public class ItemsetsFactory {
	
	// default constructor FTW
	
	private TIntArrayList buffer = new TIntArrayList();
	private int capacity = 50;
	
	public void add(int i) {
		buffer.add(i);
	}
	
	/**
	 * Resets the builder by the way
	 * @return an array containing latest items added.
	 */
	public int[] get() {
		if (capacity < buffer.size()) {
			capacity = buffer.size();
		}
		int[] res = buffer.toArray();
		buffer.clear(capacity);
		return res;
	}

	public boolean isEmpty() {
		return buffer.isEmpty();
	}
	
	/////////////////////////////////////////////////////////////////////////
	
	
	/**
	 * @return a new array concatenating each of its arguments
	 */
	public static int[] extend(int[] pattern, int extension, int[] closure) {
		int[] extended = new int[pattern.length + closure.length + 1];
		
		System.arraycopy(pattern, 0, extended, closure.length, pattern.length);
		System.arraycopy(pattern, 0, extended, closure.length, pattern.length);
		
		extended[extended.length - 1] = extension;
		
		return extended;
	}
}
