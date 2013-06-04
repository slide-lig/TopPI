package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;

import fr.liglab.lcm.internals.Counters;

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
		// O is for empty
		item++;
		if (item > Byte.MAX_VALUE) {
			item = -item + Byte.MAX_VALUE;
			if (item < Byte.MIN_VALUE) {
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
			return concatenated[this.nextPos] != 0;
		}

		@Override
		void removePosVal() {
			concatenated[this.pos] = 0;
		}

		@SuppressWarnings("cast")
		@Override
		int getPosVal() {
			if (concatenated[this.pos] > 0) {
				return concatenated[this.pos] - 1;
			} else {
				return ((int) -concatenated[this.pos]) + ((int) Byte.MAX_VALUE) - 1;
			}
		}

	}

}
