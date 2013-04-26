package fr.liglab.lcm.internals.transactions;

public abstract class TransactionsList implements Iterable<IterableTransaction> {
	private boolean sorted;

	public TransactionsList(boolean sorted) {
		this.sorted = sorted;
	}

	public final boolean isSorted() {
		return sorted;
	}

	public final void setSorted(boolean sorted) {
		this.sorted = sorted;
	}

	abstract public TransactionIterator getTransaction(final int transaction);

	abstract public TransactionsWriter getWriter();
}
