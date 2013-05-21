package fr.liglab.lcm.io;

import fr.liglab.lcm.internals.Dataset;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;

public class PerItemGroupTopKCollector extends PerItemTopKCollector {
	protected final boolean mineInGroup;
	protected final boolean mineOutGroup;
	protected TIntSet group;
	protected TIntIntMap knownBounds;

	public PerItemGroupTopKCollector(final PatternsCollector follower, int k,
			boolean mineInGroup, boolean mineOutGroup, Dataset dataset) {

		super(follower, k, dataset.counters.sortedFrequents.length,
				dataset.counters.getFrequentsIterator());

		this.mineInGroup = mineInGroup;
		this.mineOutGroup = mineOutGroup;
	}

	public final void setGroup(TIntSet group) {
		this.group = group;
	}

	public final void setKnownBounds(TIntIntMap knownBounds) {
		this.knownBounds = knownBounds;
	}

	@Override
	protected void insertPatternInTop(int support, int[] pattern, int item) {
		if (this.group.contains(item)) {
			if (!this.mineInGroup) {
				return;
			}
		} else {
			if (!this.mineOutGroup) {
				return;
			}
		}
		if (this.knownBounds != null && this.knownBounds.get(item) >= support) {
			return;
		}
		super.insertPatternInTop(support, pattern, item);
	}

	@Override
	protected int getBound(int item) {
		boolean inGroup = this.group.contains(item);
		if (!this.mineInGroup && inGroup) {
			return Integer.MAX_VALUE;
		}
		if (!this.mineOutGroup && !inGroup) {
			return Integer.MAX_VALUE;
		} else {
			return super.getBound(item);
		}
	}

	@Override
	public TIntIntMap getTopKBounds() {
		if (this.mineInGroup && this.mineOutGroup) {
			return super.getTopKBounds();
		} else if (this.mineInGroup) {
			final TIntIntHashMap bounds = new TIntIntHashMap(this.topK.size());
			TIntIterator it = this.group.iterator();
			while (it.hasNext()) {
				int item = it.next();
				int bound = 0;
				PatternWithFreq[] itemTop = this.topK.get(item);
				if (itemTop != null) {
					if (itemTop[this.k - 1] != null) {
						bound = itemTop[this.k - 1].getSupportCount();
					}
					bounds.put(item, bound);
				}
			}
			return bounds;
		} else if (this.mineOutGroup) {
			final TIntIntHashMap bounds = new TIntIntHashMap(this.topK.size());
			TIntIterator it = this.topK.keySet().iterator();
			while (it.hasNext()) {
				int item = it.next();
				if (!this.group.contains(item)) {
					int bound = 0;
					PatternWithFreq[] itemTop = this.topK.get(item);
					if (itemTop[this.k - 1] != null) {
						bound = itemTop[this.k - 1].getSupportCount();
					}
					bounds.put(item, bound);
				}
			}
			return bounds;
		} else {
			return new TIntIntHashMap();
		}
	}
}
