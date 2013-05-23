package fr.liglab.lcm.internals.tidlist;

import java.util.Arrays;

import gnu.trove.iterator.TIntIterator;

public abstract class ConcatenatedTidList extends TidList implements Cloneable {

	protected int[] concatenated;

	public static boolean compatible(int maxTid) {
		return true;
	}

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
	public TidList clone() {
		ConcatenatedTidList o = (ConcatenatedTidList) super.clone();
		o.concatenated = Arrays.copyOf(this.concatenated, this.concatenated.length);
		return o;
	}

	@Override
	public void addTransaction(final int item, final int transaction) {
		final int start = this.getPosition(item);
		if (start < 0) {
			throw new IllegalArgumentException("item " + item + " wasn't prepared");
		}
		this.concatenated[start]++;
		int length = this.concatenated[start];
		this.concatenated[start + length] = transaction;
	}

	protected abstract int getPosition(int item);

	private class ConcatenatedIterator implements TIntIterator {
		private int pos;
		private int nextPos;
		private int remaining;

		public ConcatenatedIterator(final int start) {
			this.pos = 0;
			this.nextPos = start;
			this.remaining = concatenated[start];
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
		public int next() {
			this.pos = this.nextPos;
			while (this.remaining > 0) {
				this.nextPos++;
				this.remaining--;
				if (concatenated[this.nextPos] != -1) {
					return concatenated[pos];
				}
			}
			this.nextPos = -1;
			return concatenated[pos];
		}
	}
}
