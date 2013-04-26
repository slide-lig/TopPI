package fr.liglab.lcm.internals.tidlist;

import gnu.trove.impl.Constants;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public class RandomItemsShortConcatenatedTidList extends ShortConcatenatedTidList {

	private final TIntIntMap startPositions;

	public RandomItemsShortConcatenatedTidList(final boolean sorted, final TIntIntMap lengths) {
		super(sorted);
		int startPos = 0;
		this.startPositions = new TIntIntHashMap(lengths.size(), Constants.DEFAULT_LOAD_FACTOR, -1, -1);
		TIntIntIterator iter = lengths.iterator();
		while (iter.hasNext()) {
			iter.advance();
			this.startPositions.put(iter.key(), startPos);
			startPos += (1 + iter.value());
		}
		this.concatenated = new short[startPos];
	}

	@Override
	protected int getPosition(final int item) {
		return this.startPositions.get(item);
	}

}
