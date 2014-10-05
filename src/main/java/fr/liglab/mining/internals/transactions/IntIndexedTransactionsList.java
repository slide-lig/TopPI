package fr.liglab.mining.internals.transactions;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public final class IntIndexedTransactionsList extends IndexedTransactionsList {

	public static boolean compatible(Counters c) {
		return true;
	}

	public static int getMaxTransId(Counters c) {
		return c.getDistinctTransactionsCount() - 1;
	}

	private int[] concatenated;

	public IntIndexedTransactionsList(Counters c) {
		this(c.getDistinctTransactionLengthSum(), c.getDistinctTransactionsCount());
	}

	public IntIndexedTransactionsList(int transactionsLength, int nbTransactions) {
		super(nbTransactions);
		this.concatenated = new int[transactionsLength];
	}

	@Override
	public IndexedReusableIterator getIterator() {
		return new TransIter();
	}

	@Override
	void writeItem(int item) {
		this.concatenated[this.writeIndex] = item;
		this.writeIndex++;
	}

	@Override
	public TransactionsList clone() {
		IntIndexedTransactionsList o = (IntIndexedTransactionsList) super.clone();
		o.concatenated = Arrays.copyOf(this.concatenated, this.concatenated.length);
		return o;
	}

	private final class TransIter extends BasicTransIter {

		@Override
		boolean isNextPosValid() {
			return concatenated[this.nextPos] != -1;
		}

		@Override
		void removePosVal() {
			concatenated[this.pos] = -1;
		}

		@Override
		int getPosVal() {
			return concatenated[this.pos];
		}

	}

}
