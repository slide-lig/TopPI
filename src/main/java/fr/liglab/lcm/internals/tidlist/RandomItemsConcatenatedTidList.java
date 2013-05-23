package fr.liglab.lcm.internals.tidlist;

import gnu.trove.impl.Constants;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public class RandomItemsConcatenatedTidList extends ConcatenatedTidList {

	private TIntIntMap startPositions;

	public RandomItemsConcatenatedTidList(final int[] lengths) {
		int startPos = 0;
		this.startPositions = new TIntIntHashMap(lengths.length, Constants.DEFAULT_LOAD_FACTOR, -1, -1);
		for (int i = 0; i < lengths.length; i++) {
			if (lengths[i] > 0) {
				this.startPositions.put(i, startPos);
				startPos += (1 + lengths[i]);
			}
		}
		this.concatenated = new int[startPos];
	}

	@Override
	public TidList clone() {
		RandomItemsConcatenatedTidList o = (RandomItemsConcatenatedTidList) super.clone();
		o.startPositions = new TIntIntHashMap(this.startPositions);
		return o;
	}

	public RandomItemsConcatenatedTidList(final TIntIntMap lengths) {
		int startPos = 0;
		this.startPositions = new TIntIntHashMap(lengths.size(), Constants.DEFAULT_LOAD_FACTOR, -1, -1);
		TIntIntIterator iter = lengths.iterator();
		while (iter.hasNext()) {
			iter.advance();
			this.startPositions.put(iter.key(), startPos);
			startPos += (1 + iter.value());
		}
		this.concatenated = new int[startPos];
	}

	@Override
	protected int getPosition(final int item) {
		return this.startPositions.get(item);
	}
}
