package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;
import java.util.Iterator;

import fr.liglab.lcm.internals.nomaps.Counters;

public abstract class IndexedTransactionsList extends TransactionsList {

	private int[] indexAndFreqs;
	int writeIndex = 0;
	private int size = 0;

	public IndexedTransactionsList(Counters c) {
		this(c.distinctTransactionsCount);
	}

	public IndexedTransactionsList(int nbTransactions) {
		this.indexAndFreqs = new int[2 * nbTransactions];
		Arrays.fill(this.indexAndFreqs, -1);
	}

	@Override
	public Iterator<IterableTransaction> iterator() {
		return new Iter();
	}

	@Override
	public TransactionIterator get(int transaction) {
		int startPos = 2 * transaction;
		if (transaction * 2 >= this.indexAndFreqs.length || this.indexAndFreqs[startPos] == -1) {
			throw new IllegalArgumentException("transaction " + transaction + " does not exist");
		} else {
			int endPos = transaction * 2 + 2;
			int end;
			if (endPos < this.indexAndFreqs.length) {
				end = this.indexAndFreqs[endPos];
			} else {
				end = this.writeIndex;
			}
			return this.get(this.indexAndFreqs[2 * transaction], end, transaction);
		}
	}

	abstract TransactionIterator get(int begin, int end, int transNum);

	int getTransSupport(int trans) {
		return this.indexAndFreqs[trans * 2 + 1];
	}

	void setTransSupport(int trans, int s) {
		if (s != 0 && this.indexAndFreqs[trans * 2 + 1] == 0) {
			this.size++;
		} else if (s == 0 && this.indexAndFreqs[trans * 2 + 1] != 0) {
			this.size--;
		}
		this.indexAndFreqs[trans * 2 + 1] = s;
	}

	@Override
	public TransactionsWriter getWriter() {
		return new Writer();
	}

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public TransactionsList clone() {
		IndexedTransactionsList o = (IndexedTransactionsList) super.clone();
		o.indexAndFreqs = Arrays.copyOf(this.indexAndFreqs, this.indexAndFreqs.length);
		return o;
	}

	abstract void writeItem(int item);

	private class Writer implements TransactionsWriter {
		private int transId = -1;

		@Override
		public int beginTransaction(int support) {
			this.transId++;
			indexAndFreqs[2 * this.transId] = writeIndex;
			indexAndFreqs[2 * this.transId + 1] = support;
			if (support != 0) {
				size++;
			}
			return this.transId;
		}

		@Override
		public void addItem(int item) {
			writeItem(item);
		}

		@Override
		public void endTransaction() {
		}

	}

	private class Iter implements Iterator<IterableTransaction> {
		private int pos;
		private int nextPos = -1;

		public Iter() {
			this.findNext();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public IterableTransaction next() {
			this.pos = this.nextPos;
			this.findNext();
			final int p = this.pos;
			return new IterableTransaction() {

				@Override
				public TransactionIterator iterator() {
					return get(p);
				}
			};
		}

		private void findNext() {
			while (true) {
				this.nextPos++;
				if (nextPos >= indexAndFreqs.length / 2 || indexAndFreqs[nextPos * 2] == -1) {
					this.nextPos = -1;
					return;
				}
				if (indexAndFreqs[nextPos * 2 + 1] > 0) {
					return;
				}
			}
		}

		@Override
		public boolean hasNext() {
			return this.nextPos != -1;
		}
	}

}
