package fr.liglab.lcm.internals;

public interface TransactionReader {
	public int getTransactionSupport();
	public int next();
	public boolean hasNext();
}
