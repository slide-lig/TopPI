package fr.liglab.mining.internals.transactions;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public final class UByteIndexedTransactionsList extends IndexedTransactionsList {
	private byte[] concatenated;

	@SuppressWarnings("cast")
	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() <= ((int) Byte.MAX_VALUE) - ((int) Byte.MIN_VALUE) - 1;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	public UByteIndexedTransactionsList(Counters c) {
		this(c.distinctTransactionLengthSum, c.distinctTransactionsCount);
	}

	public UByteIndexedTransactionsList(int transactionsLength, int nbTransactions) {
		super(nbTransactions);
		this.concatenated = new byte[transactionsLength];
	}

	@Override
	public IndexedReusableIterator getIterator() {
		return new TransIter();
	}

	@Override
	void writeItem(int item) {
		// Byte.MIN_VALUE is for empty
		if (item > Byte.MAX_VALUE) {
			item = -item + Byte.MAX_VALUE;
			if (item <= Byte.MIN_VALUE) {
				throw new IllegalArgumentException(item + " too big for a byte");
			}
		}
		this.concatenated[this.writeIndex] = (byte) item;
		this.writeIndex++;
	}

	@Override
	public TransactionsList clone() {
		UByteIndexedTransactionsList o = (UByteIndexedTransactionsList) super.clone();
		o.concatenated = Arrays.copyOf(this.concatenated, this.concatenated.length);
		return o;
	}

	private final class TransIter extends BasicTransIter {

		@Override
		boolean isNextPosValid() {
			return concatenated[this.nextPos] != Byte.MIN_VALUE;
		}

		@Override
		void removePosVal() {
			concatenated[this.pos] = Byte.MIN_VALUE;
		}

		@SuppressWarnings("cast")
		@Override
		int getPosVal() {
			if (concatenated[this.pos] >= 0) {
				return concatenated[this.pos];
			} else {
				return ((int) -concatenated[this.pos]) + ((int) Byte.MAX_VALUE);
			}
		}

	}

}
