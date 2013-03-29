package fr.liglab.lcm.io;

import fr.liglab.lcm.internals.Dataset;
import gnu.trove.iterator.TIntIterator;

public final class PerItemTopKCollectorThreadSafeInitialized extends
		PerItemTopKCollector {
	/*
	 * If we go for threads, how do we make this threadsafe ? If this is not a
	 * bottleneck, synchronized on collect is ok Else, if we want parallelism,
	 * we need to take a lock when dealing with an item We can use a collection
	 * of locks and a hash of the item to do that safely, be careful with
	 * insertions in the map though
	 * 
	 * What we should do for MapReduce is create it in the open and close it on
	 * the close, so when we execute several maps on the same mapper they all
	 * benefit from the top-k of the others
	 */
	public PerItemTopKCollectorThreadSafeInitialized(
			final PatternsCollector decorated, Dataset dataset, final int k) {
		this(decorated, k, dataset, false);
	}

	public PerItemTopKCollectorThreadSafeInitialized(
			final PatternsCollector follower, final int k, Dataset dataset,
			final boolean outputEachPatternOnce) {
		this(follower, k, dataset.getSupportCounts().keySet().iterator(),
				outputEachPatternOnce);
	}

	public PerItemTopKCollectorThreadSafeInitialized(
			final PatternsCollector follower, final int k, TIntIterator items,
			final boolean outputEachPatternOnce) {
		super(follower, k, outputEachPatternOnce);
		// we may want to hint a default size, it is at least the group size,
		// but in practice much bigger
		this.init(items);
	}

	private void init(final TIntIterator items) {
		while (items.hasNext()) {
			this.topK.put(items.next(), new PatternWithFreq[k]);
		}
	}

	@Override
	protected void insertPatternInTop(final int support, final int[] pattern,
			final int item) {
		PatternWithFreq[] itemTopK = this.topK.get(item);
		if (itemTopK == null) {
			throw new RuntimeException("item not initialized " + item);
		} else {
			synchronized (itemTopK) {
				super.updateTop(support, pattern, itemTopK);
			}
		}
	}
}
