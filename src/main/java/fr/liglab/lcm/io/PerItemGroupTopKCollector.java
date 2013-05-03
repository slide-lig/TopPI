package fr.liglab.lcm.io;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;

public class PerItemGroupTopKCollector extends PerItemTopKCollector {
	protected final boolean mineInGroup;
	protected final boolean mineOutGroup;
	protected TIntSet group;
	protected TIntIntMap knownBounds;

	public PerItemGroupTopKCollector(final PatternsCollector follower, int k) {
		this(follower, k, true, true);
	}

	public PerItemGroupTopKCollector(final PatternsCollector follower, int k, boolean mineInGroup, boolean mineOutGroup) {
		super(follower, k, false);
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
	protected int checkExploreOtherItem(final int item, final int itemSupport, final int extension,
			final int extensionSupport, final TIntIntMap failedPPTests) {
		if (!this.mineInGroup && this.group.contains(item)) {
			return Integer.MAX_VALUE;
		} else if (!this.mineOutGroup && !this.group.contains(item)) {
			return Integer.MAX_VALUE;
		} else if (this.knownBounds != null) {
			int knownBound = this.knownBounds.get(item);
			if (knownBound >= Math.min(extensionSupport, itemSupport)) {
				return knownBound;
			}
		}
		return super.checkExploreOtherItem(item, itemSupport, extension, extensionSupport, failedPPTests);
	}

	@Override
	protected int checkExploreInCurrentPattern(int item, int itemSupport) {
		if (!this.mineInGroup && this.group.contains(item)) {
			return Integer.MAX_VALUE;
		} else if (!this.mineOutGroup && !this.group.contains(item)) {
			return Integer.MAX_VALUE;
		} else if (this.knownBounds != null) {
			int knownBound = this.knownBounds.get(item);
			if (knownBound >= itemSupport) {
				return knownBound;
			}
		}
		return super.checkExploreInCurrentPattern(item, itemSupport);
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
