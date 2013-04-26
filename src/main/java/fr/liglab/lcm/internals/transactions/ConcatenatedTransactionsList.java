package fr.liglab.lcm.internals.transactions;

import java.util.Iterator;

public class ConcatenatedTransactionsList extends TransactionsList {
	public ConcatenatedTransactionsList(boolean sorted, int transactionsLength, int nbTransactions) {
		super(sorted);
		this.concatenated = new int[transactionsLength + nbTransactions];
	}

	private int[] concatenated;

	@Override
	public Iterator<IterableTransaction> iterator() {
		return new TransIter();
	}

	@Override
	public TransactionIterator getTransaction(int transaction) {
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

	private final class TransComp extends TransactionIterator {
		private int length;
		private int pos;
		private int nextPos;
		private final int startPos;

		public TransComp(int startPos) {
			this.startPos = startPos;
			this.nextPos = startPos;
			this.length = concatenated[this.nextPos];
			this.nextPos++;
			while (this.length > 0) {
				this.nextPos++;
				if (concatenated[this.nextPos] != -1) {
					return;
				}
			}
			this.nextPos = -1;
		}

		@Override
		public int next() {
			this.pos = this.nextPos;
			while (this.length > 0) {
				this.nextPos++;
				if (concatenated[this.nextPos] != -1) {
					return concatenated[pos];
				}
			}
			this.nextPos = -1;
			return concatenated[pos];
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
			concatenated[this.startPos + 1] = s;
		}
	}

	private final class TransIter implements Iterator<IterableTransaction> {
		private int support;
		private int length;
		private int pos;
		private int nextPos;

		public TransIter() {
			if (concatenated.length < 2) {
				this.nextPos = -1;
				return;
			} else {
				this.pos = 0;
				this.nextPos = 0;
				this.length = concatenated[this.nextPos];
				this.support = concatenated[this.nextPos + 1];
				while (true) {
					if (this.support > 0) {
						return;
					} else {
						this.nextPos += this.length + 2;
						if (this.nextPos >= concatenated.length) {
							this.nextPos = -1;
							return;
						} else {
							this.length = concatenated[this.nextPos];
							this.support = concatenated[this.nextPos + 1];
						}
					}
				}
			}
		}

		@Override
		public boolean hasNext() {
			return this.nextPos >= 0;
		}

		@Override
		public IterableTransaction next() {
			this.pos = this.nextPos;
			this.nextPos += this.length + 2;
			if (this.nextPos >= concatenated.length) {
				this.nextPos = -1;
				return new IterableTrans(this.pos);
			} else {
				while (true) {
					if (this.support > 0) {
						return new IterableTrans(this.pos);
					} else {
						this.nextPos += this.length + 2;
						if (this.nextPos >= concatenated.length) {
							this.nextPos = -1;
							return new IterableTrans(this.pos);
						} else {
							this.length = concatenated[this.nextPos];
							this.support = concatenated[this.nextPos + 1];
						}
					}
				}
			}
		}

		@Override
		public void remove() {
			concatenated[this.pos + 1] = 0;
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
