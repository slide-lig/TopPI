package fr.liglab.lcm.internals.transactions;

import java.util.Arrays;
import java.util.Iterator;

import fr.liglab.lcm.internals.nomaps.Counters;

public class ConcatenatedTransactionsList extends TransactionsList {
	public static boolean compatible(Counters c) {
		return true;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionLengthSum + c.distinctTransactionsCount * 2 - 1;
	}

	private int[] concatenated;
	private int size;

	public ConcatenatedTransactionsList(int transactionsLength, int nbTransactions) {
		this.concatenated = new int[transactionsLength + 2 * nbTransactions];
	}

	@Override
	public TransactionsList clone() {
		ConcatenatedTransactionsList o = (ConcatenatedTransactionsList) super.clone();
		o.concatenated = Arrays.copyOf(this.concatenated, this.concatenated.length);
		return o;
	}

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public Iterator<IterableTransaction> iterator() {
		return new TransIter();
	}

	@Override
	public TransactionIterator get(int transaction) {
		if (transaction >= this.concatenated.length) {
			throw new IllegalArgumentException("transaction " + transaction + " does not exist");
		}
		return new TransComp(transaction);
	}

	@Override
	public TransactionsWriter getWriter() {
		return new TransactionsWriter() {
			private int index;
			private int lastTransactionLength;
			private int lastTransactionId;

			@Override
			public void endTransaction() {
				size++;
				concatenated[this.lastTransactionId] = this.lastTransactionLength;
			}

			@Override
			public int beginTransaction(int support) {
				this.lastTransactionId = index;
				index++;
				concatenated[index] = support;
				index++;
				this.lastTransactionLength = 0;
				return this.lastTransactionId;
			}

			@Override
			public void addItem(int item) {
				concatenated[this.index] = item;
				this.lastTransactionLength++;
				this.index++;
			}
		};
	}

	private final class TransComp implements TransactionIterator {
		private int remaining;
		private int pos;
		private int nextPos;
		private final int startPos;

		public TransComp(int startPos) {
			this.startPos = startPos;
			this.nextPos = startPos;
			this.remaining = concatenated[this.nextPos];
			this.nextPos++;
			findNext();
		}

		@Override
		public int next() {
			this.pos = this.nextPos;
			findNext();
			return concatenated[pos];
		}

		private void findNext() {
			while (this.remaining > 0) {
				this.nextPos++;
				this.remaining--;
				if (concatenated[this.nextPos] != -1) {
					return;
				}
			}
			this.nextPos = -1;
		}

		@Override
		public void remove() {
			concatenated[pos] = -1;
		}

		@Override
		public boolean hasNext() {
			return this.nextPos >= 0;
		}

		@Override
		public int getTransactionSupport() {
			return concatenated[this.startPos + 1];
		}

		@Override
		public void setTransactionSupport(int s) {
			if (s <= 0) {
				size--;
			}
			concatenated[this.startPos + 1] = s;
		}
	}

	private final class TransIter implements Iterator<IterableTransaction> {
		private int support;
		private int length;
		private int pos;

		public TransIter() {
			if (concatenated.length < 2) {
				this.support = 0;
				return;
			} else {
				this.findNext(true);
			}
		}

		private void findNext(boolean firstTime) {
			while (true) {
				if (firstTime) {
					firstTime = false;
				} else {
					this.pos += 2 + this.length;
				}
				if (this.pos >= concatenated.length) {
					this.support = 0;
					return;
				}
				this.length = concatenated[this.pos];
				this.support = concatenated[this.pos + 1];
				if (this.support != 0) {
					return;
				}
			}
		}

		@Override
		public boolean hasNext() {
			return this.support > 0;
		}

		@Override
		public IterableTransaction next() {
			IterableTransaction res = new IterableTrans(this.pos);
			this.findNext(false);
			return res;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	private final class IterableTrans implements IterableTransaction {
		private final int pos;

		public IterableTrans(final int pos) {
			this.pos = pos;
		}

		@Override
		public TransactionIterator iterator() {
			return new TransComp(this.pos);
		}

	}
}
