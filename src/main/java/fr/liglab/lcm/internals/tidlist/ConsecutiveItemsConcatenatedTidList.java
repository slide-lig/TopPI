package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;

public class ConsecutiveItemsConcatenatedTidList extends ConcatenatedTidList {

	private final int[] startPositions;

	public ConsecutiveItemsConcatenatedTidList(final boolean sorted, final TIntIntMap lengths) {
		super(sorted);
		int startPos = 0;
		this.startPositions = new int[lengths.size()];
		TIntIntIterator iter = lengths.iterator();
		while (iter.hasNext()) {
			iter.advance();
			this.startPositions[iter.key()] = startPos;
			startPos += (1 + iter.value());
		}
		this.concatenated = new int[startPos];
	}

	@Override
	protected int getPosition(final int item) {
		if (item >= this.startPositions.length) {
			return -1;
		} else {
			return this.startPositions[item];
		}
	}

}
