package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;

import fr.liglab.lcm.internals.Counters;

public final class UShortIndexedTransactionsList extends IndexedTransactionsList {
	private char[] concatenated;

	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() < Character.MAX_VALUE;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	public UShortIndexedTransactionsList(Counters c) {
		this(c.distinctTransactionLengthSum, c.distinctTransactionsCount);
	}

	public UShortIndexedTransactionsList(int transactionsLength, int nbTransactions) {
		super(nbTransactions);
		this.concatenated = new char[transactionsLength];
	}

	@Override
	void writeItem(int item) {
		// MAX_VALUE is for empty;
		if (item == Character.MAX_VALUE) {
			throw new IllegalArgumentException(item + " too big for a char");
		}
		this.concatenated[this.writeIndex] = (char) item;
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
			return concatenated[this.nextPos] != Character.MAX_VALUE;
		}

		@Override
		void removePosVal() {
			concatenated[this.pos] = Character.MAX_VALUE;
		}

		@Override
		int getPosVal() {
			return concatenated[this.pos];
		}

	}
}
