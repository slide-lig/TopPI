package fr.liglab.lcm.internals.tidlist;

import fr.liglab.lcm.internals.nomaps.Counters;
import gnu.trove.impl.Constants;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public abstract class RandomItemsConcatenatedTidList extends TidList {

	private TIntIntMap startPositions;

	private TIntIntMap indexes;

	public RandomItemsConcatenatedTidList(Counters c) {
		this(c.distinctTransactionsCounts);
	}

	public RandomItemsConcatenatedTidList(final int[] lengths) {
		int startPos = 0;
		this.startPositions = new TIntIntHashMap(lengths.length, Constants.DEFAULT_LOAD_FACTOR, -1, -1);
		this.indexes = new TIntIntHashMap(lengths.length);
		for (int i = 0; i < lengths.length; i++) {
			this.startPositions.put(i, startPos);
			startPos += (1 + lengths[i]);
		}
		this.allocateArray(startPos);
	}

	abstract void allocateArray(int size);

	@Override
	public TidList clone() {
		RandomItemsConcatenatedTidList o = (RandomItemsConcatenatedTidList) super.clone();
		o.startPositions = new TIntIntHashMap(this.startPositions);
		o.indexes = new TIntIntHashMap(o.indexes);
		return o;
	}

	@Override
	public TIntIterator get(final int item) {
		final int startPos = this.startPositions.get(item);
		if (startPos == -1) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		}
		final int length = this.indexes.get(item);
		return new TIntIterator() {
			int index = 0;

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean hasNext() {
				return this.index < length;
			}

			@Override
			public int next() {
				int res = read(startPos + index);
				this.index++;
				return res;
			}
		};
	}

	@Override
	public TIntIterable getIterable(final int item) {
		return new TIntIterable() {

			@Override
			public TIntIterator iterator() {
				return get(item);
			}
		};
	}

	@Override
	public void addTransaction(int item, int transaction) {
		final int startPos = this.startPositions.get(item);
		if (startPos == -1) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		}
		int index = this.indexes.get(item);
		this.write(startPos + index, transaction);
		this.indexes.adjustOrPutValue(item, 1, 1);
	}

	abstract void write(int position, int transaction);

	abstract int read(int position);
}
