package fr.liglab.lcm.internals.tidlist;

import gnu.trove.impl.Constants;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

public class RandomItemsShortConcatenatedTidList extends ShortConcatenatedTidList {

	private final TIntIntMap startPositions;

	public RandomItemsShortConcatenatedTidList(final int[] lengths) {
		int startPos = 0;
		this.startPositions = new TIntIntHashMap(lengths.length, Constants.DEFAULT_LOAD_FACTOR, -1, -1);
		for (int i = 0; i < lengths.length; i++) {
			this.startPositions.put(i, startPos);
			startPos += (1 + lengths[i]);
		}
		this.concatenated = new short[startPos];
	}

	@Override
	protected int getPosition(final int item) {
		return this.startPositions.get(item);
	}

}
