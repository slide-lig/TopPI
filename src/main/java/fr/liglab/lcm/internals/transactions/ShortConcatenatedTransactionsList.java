package fr.liglab.lcm.internals.transactions;

import java.util.Iterator;

import fr.liglab.lcm.internals.nomaps.Counters;

public class ShortConcatenatedTransactionsList extends TransactionsList {
	@SuppressWarnings("cast")
	public static boolean compatible(Counters c) {
		return c.getMaxFrequent() <= ((int) Short.MAX_VALUE) - ((int) Short.MIN_VALUE) - 1;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionLengthSum + c.distinctTransactionsCount * 4 - 1;
	}

	private short[] concatenated;
	private int size;

	public ShortConcatenatedTransactionsList(int transactionsLength, int nbTransactions) {
		this.concatenated = new short[transactionsLength + 4 * nbTransactions];
	}

	private int readInt(int pos) {
		return concatenated[pos] << 16 | (concatenated[pos + 1] & 0xFFFF);
	}

	private void writeInt(int pos, int val) {
		this.concatenated[pos] = (short) (val >> 16);
		this.concatenated[pos + 1] = (short) val;
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
				writeInt(this.lastTransactionId, this.lastTransactionLength);
			}

			@Override
			public int beginTransaction(int support) {
				this.lastTransactionId = index;
				index += 2;
				writeInt(index, support);
				index += 2;
				this.lastTransactionLength = 0;
				return this.lastTransactionId;
			}

			@Override
			public void addItem(int item) {
				// O is for empty
				item++;
				if (item > Short.MAX_VALUE) {
					item = -item + Short.MAX_VALUE;
					if (item < Short.MIN_VALUE) {
						throw new IllegalArgumentException(item + " too big for a short");
					}
				}
				concatenated[this.index] = (short) item;
				this.lastTransactionLength++;
				this.index++;
			}
		};
	}

	private final class TransComp extends TransactionIterator {
		private int remaining;
		private int pos;
		private int nextPos;
		private final int startPos;

		public TransComp(int startPos) {
			this.startPos = startPos;
			this.nextPos = startPos;
			this.remaining = readInt(this.nextPos);
			// skip over length and half of support
			this.nextPos += 3;
			findNext();
		}

		@SuppressWarnings("cast")
		@Override
		public int next() {
			this.pos = this.nextPos;
			findNext();
			if (concatenated[this.pos] >= 0) {
				return concatenated[this.pos] - 1;
			} else {
				return ((int) -concatenated[this.pos]) + ((int) Short.MAX_VALUE) - 1;
			}
		}

		private void findNext() {
			while (this.remaining > 0) {
				this.nextPos++;
				this.remaining--;
				if (concatenated[this.nextPos] != 0) {
					return;
				}
			}
			this.nextPos = -1;
		}

		@Override
		public void remove() {
			concatenated[pos] = 0;
		}

		@Override
		public boolean hasNext() {
			return this.nextPos >= 0;
		}

		@Override
		public int getTransactionSupport() {
			return readInt(this.startPos + 2);
		}

		@Override
		public void setTransactionSupport(int s) {
			if (s <= 0) {
				size--;
			}
			writeInt(this.startPos + 2, s);
		}
	}

	private final class TransIter implements Iterator<IterableTransaction> {
		private int support;
		private int length;
		private int pos;

		public TransIter() {
			if (concatenated.length < 4) {
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
					this.pos += 4 + this.length;
				}
				if (this.pos >= concatenated.length) {
					this.support = 0;
					return;
				}
				this.length = readInt(this.pos);
				this.support = readInt(this.pos + 2);
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
