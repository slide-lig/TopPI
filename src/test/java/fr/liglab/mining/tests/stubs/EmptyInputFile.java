package fr.liglab.mining.tests.stubs;

import java.util.Iterator;

import fr.liglab.mining.internals.TransactionReader;

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
