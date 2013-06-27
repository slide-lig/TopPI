package fr.liglab.mining.internals.transactions;

import fr.liglab.mining.internals.TransactionReader;
import gnu.trove.iterator.TIntIterator;

/**
 * FIXME - should not implement two overlapping interfaces --> why ? - comment
 */
public interface TransactionIterator extends TransactionReader, TIntIterator {
	public void setTransactionSupport(int s);

	public void remove();
}
