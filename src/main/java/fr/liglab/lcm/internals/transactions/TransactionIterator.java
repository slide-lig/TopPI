package fr.liglab.lcm.internals.transactions;

import fr.liglab.lcm.internals.TransactionReader;
import gnu.trove.iterator.TIntIterator;

public abstract class TransactionIterator implements TransactionReader, TIntIterator {
	public abstract void setTransactionSupport(int s);

	public abstract void remove();
}
