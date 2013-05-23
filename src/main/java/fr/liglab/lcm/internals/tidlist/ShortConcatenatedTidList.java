package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIterator;

public abstract class ShortConcatenatedTidList extends TidList {

	@SuppressWarnings("cast")
	public static boolean compatible(int maxTid) {
		return maxTid <= ((int) Short.MAX_VALUE) - ((int) Short.MIN_VALUE);
	}

	protected short[] concatenated;

	@Override
	public TIntIterator get(final int item) {
		final int start = this.getPosition(item);
		if (start < 0) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			return new ConcatenatedIterator(start);
		}
	}

	@Override
	public TIntIterable getIterable(int item) {
		final int start = this.getPosition(item);
		if (start < 0) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			return new TIntIterable() {

				@Override
				public TIntIterator iterator() {
					return new ConcatenatedIterator(start);
				}
			};
		}
	}

	@Override
	public void addTransaction(final int item, int transaction) {
		if (transaction > Short.MAX_VALUE) {
			transaction = -transaction + Short.MAX_VALUE;
			if (transaction < Short.MIN_VALUE) {
				throw new IllegalArgumentException(transaction + " too big for a short");
			}
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
		int length;

		public ConcatenatedIterator(final int start) {
			this.pos = start;
			this.length = concatenated[start];
			this.pos++;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean hasNext() {
			return this.length > 0;
		}

		@SuppressWarnings("cast")
		@Override
		public int next() {
			this.length--;
			int v;
			if (concatenated[this.pos] >= 0) {
				v = concatenated[pos];
			} else {
				v = ((int) -concatenated[pos]) + ((int) Short.MAX_VALUE);
			}
			pos++;
			return v;
		}
	}
}
