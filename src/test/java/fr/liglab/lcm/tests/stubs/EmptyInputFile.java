package fr.liglab.lcm.tests.stubs;

import java.util.Iterator;

/**
 * 
 * @author martin
 *
 */
public class EmptyInputFile implements Iterator<int[]> {

	public boolean hasNext() {
		return false;
	}

	public int[] next() {
		return null;
	}

	public void remove() {
		
	}
}
