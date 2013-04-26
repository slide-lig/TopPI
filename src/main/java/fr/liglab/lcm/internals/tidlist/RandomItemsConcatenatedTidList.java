package fr.liglab.lcm.internals.tidlist;

import gnu.trove.impl.Constants;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public class RandomItemsConcatenatedTidList extends ConcatenatedTidList {

	private final TIntIntMap startPositions;

	public RandomItemsConcatenatedTidList(final boolean sorted, final TIntIntMap lengths) {
		super(sorted);
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
