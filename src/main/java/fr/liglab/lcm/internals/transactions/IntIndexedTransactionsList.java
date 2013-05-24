package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;

import fr.liglab.lcm.internals.nomaps.Counters;

public class IntIndexedTransactionsList extends IndexedTransactionsList {

	public static boolean compatible(Counters c) {
		return true;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	private int[] concatenated;

	public IntIndexedTransactionsList(int transactionsLength, int nbTransactions) {
		super(nbTransactions);
		this.concatenated = new int[transactionsLength];
	}

	@Override
	TransactionIterator get(int begin, int end, int transNum) {
		return new TransIter(begin, end, transNum);
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

	private class TransIter implements TransactionIterator {

		private int transNum;
		private int pos;
		private int nextPos;
		private int end;

		public TransIter(int begin, int end, int transNum) {
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
				if (concatenated[nextPos] != -1) {
					return;
				}
			}
		}

		@Override
		public int getTransactionSupport() {
			return getTransSupport(transNum);
		}

		@Override
		public int next() {
			this.pos = this.nextPos;
			this.findNext();
			return concatenated[this.pos];
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
			concatenated[this.pos] = -1;
		}

	}

}
