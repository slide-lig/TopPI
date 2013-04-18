package fr.liglab.lcm.util;

import gnu.trove.list.array.TIntArrayList;

/**
 * Itemsets and patterns are represented by classic integer arrays
 * Aside static utility methods, instanciate it to create arrays without knowing their length beforehand
 */
public class ItemsetsFactory {
	
	// default constructor FTW
	
	protected TIntArrayList buffer = new TIntArrayList();
	protected int capacity = 50;
	
	/**
	 * If you're going big and have an estimation of future array's size...
	 */
	public void ensureCapacity(final int c) {
		buffer.ensureCapacity(c);
	}
	
	public void add(final int i) {
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
	public static int[] extend(final int[] pattern, final int extension, final int[] closure) {
		if (closure == null) {
			return extend(pattern, extension);
		}
		
		int[] extended = new int[pattern.length + closure.length + 1];
		
		System.arraycopy(pattern, 0, extended, 0, pattern.length);
		extended[pattern.length] = extension;
		System.arraycopy(closure, 0, extended, pattern.length + 1, closure.length);

		return extended;
	}

	public static int[] extend(final int[] pattern, final int extension, final int[] closure, final int[] ignoreItems) {
		if (ignoreItems == null) {
			return extend(pattern, extension, closure);
		}
		
		int[] extended = new int[pattern.length + closure.length + 1 + ignoreItems.length];

		System.arraycopy(pattern, 0, extended, 0, pattern.length);
		extended[pattern.length] = extension;
		System.arraycopy(closure, 0, extended, pattern.length + 1, closure.length);
		System.arraycopy(ignoreItems, 0, extended, pattern.length + 1 + closure.length, ignoreItems.length);
		System.arraycopy(closure, 0, extended, pattern.length+1, closure.length);
		
		return extended;
	}

	/**
	 * @return a new array concatenating each of its arguments
	 */
	public static int[] extend(final int[] closure, final int extension) {
		int[] extended = new int[closure.length + 1];
		
		System.arraycopy(closure, 0, extended, 0, closure.length);
		extended[closure.length] = extension;
		
		return extended;
	}
}
