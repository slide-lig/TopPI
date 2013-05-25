package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;

import fr.liglab.lcm.internals.Counters;

public class ShortIndexedTransactionsList extends IndexedTransactionsList {
	private short[] concatenated;

	@SuppressWarnings("cast")
	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() <= ((int) Short.MAX_VALUE) - ((int) Short.MIN_VALUE) - 1;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	public ShortIndexedTransactionsList(Counters c) {
		this(c.distinctTransactionLengthSum, c.distinctTransactionsCount);
	}

	public ShortIndexedTransactionsList(int transactionsLength, int nbTransactions) {
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
		ShortIndexedTransactionsList o = (ShortIndexedTransactionsList) super.clone();
		o.concatenated = Arrays.copyOf(this.concatenated, this.concatenated.length);
		return o;
	}

	@Override
	public IndexedReusableIterator getIterator() {
		return new TransIter();
	}

	private class TransIter extends IndexedReusableIterator {

		private int transNum;
		private int pos;
		private int nextPos;
		private int end;

		public TransIter() {
		}

		@Override
		void set(int begin, int end, int transNum) {
			this.transNum = transNum;
			this.nextPos = begin - 1;
			this.end = end;
			this.findNext();
		}

		private void findNext() {
			while (true) {
				this.nextPos++;
				if (nextPos == this.end) {
					this.nextPos = -1;
					return;
				}
				if (concatenated[nextPos] != 0) {
					return;
				}
			}
		}

		@Override
		public int getTransactionSupport() {
			return getTransSupport(transNum);
		}

		@SuppressWarnings("cast")
		@Override
		public int next() {
			this.pos = this.nextPos;
			this.findNext();
			if (concatenated[this.pos] > 0) {
				return concatenated[this.pos] - 1;
			} else {
				return ((int) -concatenated[this.pos]) + ((int) Short.MAX_VALUE) - 1;
			}
		}

		@Override
		public boolean hasNext() {
			return this.nextPos != -1;
		}

		@Override
		public void setTransactionSupport(int s) {
			setTransSupport(this.transNum, s);
		}

		@Override
		public void remove() {
			concatenated[this.pos] = 0;
		}

	}

}
