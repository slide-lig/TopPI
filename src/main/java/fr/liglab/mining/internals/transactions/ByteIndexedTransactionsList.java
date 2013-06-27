package fr.liglab.mining.internals.transactions;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public final class ByteIndexedTransactionsList extends IndexedTransactionsList {

	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() <= Byte.MAX_VALUE;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	private byte[] concatenated;

	public ByteIndexedTransactionsList(Counters c) {
		this(c.distinctTransactionLengthSum, c.distinctTransactionsCount);
	}

	public ByteIndexedTransactionsList(int transactionsLength, int nbTransactions) {
		super(nbTransactions);
		this.concatenated = new byte[transactionsLength];
	}

	@Override
	public IndexedReusableIterator getIterator() {
		return new TransIter();
	}

	@Override
	void writeItem(int item) {
		if (item > Byte.MAX_VALUE) {
			throw new IllegalArgumentException(item + " too big for a short");
		}
		this.concatenated[this.writeIndex] = (byte) item;
		this.writeIndex++;
	}

	@Override
	public TransactionsList clone() {
		ByteIndexedTransactionsList o = (ByteIndexedTransactionsList) super.clone();
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
