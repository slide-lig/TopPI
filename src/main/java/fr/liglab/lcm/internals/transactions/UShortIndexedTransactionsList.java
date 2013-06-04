package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;

import fr.liglab.lcm.internals.Counters;

public final class UShortIndexedTransactionsList extends IndexedTransactionsList {
	private short[] concatenated;

	@SuppressWarnings("cast")
	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() <= ((int) Short.MAX_VALUE) - ((int) Short.MIN_VALUE) - 1;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	public UShortIndexedTransactionsList(Counters c) {
		this(c.distinctTransactionLengthSum, c.distinctTransactionsCount);
	}

	public UShortIndexedTransactionsList(int transactionsLength, int nbTransactions) {
		super(nbTransactions);
		this.concatenated = new short[transactionsLength];
	}

	@Override
	void writeItem(int item) {
		// O is for empty
		item++;
		if (item > Short.MAX_VALUE) {
			item = -item + Short.MAX_VALUE;
			if (item < Short.MIN_VALUE) {
				throw new IllegalArgumentException(item + " too big for a short");
			}
		}
		this.concatenated[this.writeIndex] = (short) item;
		this.writeIndex++;
	}

	@Override
	public TransactionsList clone() {
		UShortIndexedTransactionsList o = (UShortIndexedTransactionsList) super.clone();
		o.concatenated = Arrays.copyOf(this.concatenated, this.concatenated.length);
		return o;
	}

	@Override
	public IndexedReusableIterator getIterator() {
		return new TransIter();
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
				return ((int) -concatenated[this.pos]) + ((int) Short.MAX_VALUE) - 1;
			}
		}

	}
}
