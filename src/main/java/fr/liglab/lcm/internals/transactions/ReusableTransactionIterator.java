package fr.liglab.lcm.internals.transactions;

public interface ReusableTransactionIterator extends TransactionIterator {
	public void setTransaction(int transaction);
}
