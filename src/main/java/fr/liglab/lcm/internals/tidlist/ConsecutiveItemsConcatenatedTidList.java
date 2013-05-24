package fr.liglab.lcm.internals.tidlist;

import fr.liglab.lcm.internals.nomaps.Counters;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;

import java.util.Arrays;

public class ConsecutiveItemsConcatenatedTidList extends ConcatenatedTidList {

	private int[] startPositions;

	public ConsecutiveItemsConcatenatedTidList(Counters c) {
		this(c.distinctTransactionsCounts);
	}

	public ConsecutiveItemsConcatenatedTidList(final int[] lengths) {
		int startPos = 0;
		this.startPositions = new int[lengths.length];
		for (int i = 0; i < lengths.length; i++) {
			if (lengths[i] > 0) {
				this.startPositions[i] = startPos;
				startPos += (1 + lengths[i]);
			}
		}
		this.concatenated = new int[startPos];
	}

	public ConsecutiveItemsConcatenatedTidList(final TIntIntMap lengths) {
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
	public TidList clone() {
		ConsecutiveItemsConcatenatedTidList o = (ConsecutiveItemsConcatenatedTidList) super.clone();
		o.startPositions = Arrays.copyOf(this.startPositions, this.startPositions.length);
		return o;
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
