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
				// we do not have k patterns for this item yet
				if (itemTopK[this.k - 1] == null) {
					// find the position of the last null entry
					int lastNull = k - 1;
					while (lastNull > 0 && itemTopK[lastNull - 1] == null) {
						lastNull--;
					}
					// now compare with the valid entries to adjust position
					int newPosition = lastNull;
					while (newPosition >= 1) {
						if (itemTopK[newPosition - 1].getSupportCount() < support) {
							newPosition--;
						} else {
							break;
						}
					}
					// make room for the new pattern
					for (int i = lastNull; i > newPosition; i--) {
						itemTopK[i] = itemTopK[i - 1];
					}
					// insert the new pattern where previously computed
					itemTopK[newPosition] = new PatternWithFreq(support,
							pattern);
				} else
				// the support of the new pattern is higher than the kth
				// previously
				// known
				if (itemTopK[this.k - 1].getSupportCount() < support) {
					// find where the new pattern is going to be inserted in the
					// sorted topk list
					int newPosition = k - 1;
					while (newPosition >= 1) {
						if (itemTopK[newPosition - 1].getSupportCount() < support) {
							newPosition--;
						} else {
							break;
						}
					}
					// make room for the new pattern, evicting the one at the
					// end
					for (int i = this.k - 1; i > newPosition; i--) {
						itemTopK[i] = itemTopK[i - 1];
					}
					// insert the new pattern where previously computed
					itemTopK[newPosition] = new PatternWithFreq(support,
							pattern);
				}
				// else not in top k for this item, do nothing
			}
		}
	}
}
