package fr.liglab.mining.internals.transactions;

public interface TransactionsWriter {
	public int beginTransaction(int support);

	public void addItem(int item);

	public void endTransaction();
}
