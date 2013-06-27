package fr.liglab.mining.internals.transactions;

public interface ReusableTransactionIterator extends TransactionIterator {
	public void setTransaction(int transaction);
}
