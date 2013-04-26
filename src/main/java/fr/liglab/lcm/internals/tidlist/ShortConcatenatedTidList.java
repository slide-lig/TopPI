package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIterator;

public abstract class ShortConcatenatedTidList extends TidList {

	protected short[] concatenated;

	public ShortConcatenatedTidList(boolean sorted) {
		super(sorted);
	}

	@Override
	public TIntIterator getTidList(final int item) {
		final int start = this.getPosition(item);
		if (start < 0) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			return new ConcatenatedIterator(start);
		}
	}

	@Override
	public void addTransaction(final int item, final int transaction) {
		if (transaction > Short.MAX_VALUE) {
			throw new IllegalArgumentException(transaction + " too big for a short");
		}
		final int start = this.getPosition(item);
		if (start < 0) {
			throw new IllegalArgumentException("item " + item + " wasn't prepared");
		}
		this.concatenated[start]++;
		int length = this.concatenated[start];
		this.concatenated[start + length] = (short) transaction;
	}

	protected abstract int getPosition(int item);

	private class ConcatenatedIterator implements TIntIterator {
		int pos;
		int nextPos;
		int length;

		public ConcatenatedIterator(final int start) {
			this.pos = 0;
			this.nextPos = start;
			this.length = concatenated[start];
			while (this.length > 0) {
				this.nextPos++;
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
	}
}
