package fr.liglab.lcm.tests.stubs;

import java.util.Iterator;

import fr.liglab.lcm.internals.TransactionReader;

/**
 * 
 * @author martin
 *
 */
public class EmptyInputFile implements Iterator<TransactionReader> {

	public boolean hasNext() {
		return false;
	}

	public TransactionReader next() {
		return null;
	}

	public void remove() {
		
	}
}
