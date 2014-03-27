package fr.liglab.mining.internals.transactions;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public final class ShortIndexedTransactionsList extends IndexedTransactionsList {

	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() <= Short.MAX_VALUE;
	}

	public static int getMaxTransId(Counters c) {
		return c.getDistinctTransactionsCount() - 1;
	}

	private short[] concatenated;

	public ShortIndexedTransactionsList(Counters c) {
		this(c.getDistinctTransactionLengthSum(), c.getDistinctTransactionsCount());
	}

	public ShortIndexedTransactionsList(int transactionsLength, int nbTransactions) {
		super(nbTransactions);
		this.concatenated = new short[transactionsLength];
	}

	@Override
	public IndexedReusableIterator getIterator() {
		return new TransIter();
	}

	@Override
	void writeItem(int item) {
		if (item > Short.MAX_VALUE) {
			throw new IllegalArgumentException(item + " too big for a short");
		}
		this.concatenated[this.writeIndex] = (short) item;
		this.writeIndex++;
	}

	@Override
	public TransactionsList clone() {
		ShortIndexedTransactionsList o = (ShortIndexedTransactionsList) super.clone();
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
