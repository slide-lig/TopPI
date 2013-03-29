package fr.liglab.lcm.io;

import gnu.trove.map.TIntIntMap;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class PerItemTopKCollectorRWLock extends PerItemTopKCollector {
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
	private final ReadWriteLock mapLock;

	public PerItemTopKCollectorRWLock(final PatternsCollector decorated,
			final int k) {
		this(decorated, k, false);
	}

	public PerItemTopKCollectorRWLock(final PatternsCollector follower,
			final int k, final boolean outputEachPatternOnce) {
		super(follower, k, outputEachPatternOnce);
		this.mapLock = new ReentrantReadWriteLock();
	}

	@Override
	protected void insertPatternInTop(int support, int[] pattern, int item) {
		PatternWithFreq[] itemTopK = this.topK.get(item);
		// first item in topk
		if (itemTopK == null) {
			// pas besoin du lock sur l'item
			this.mapLock.readLock().unlock();
			this.mapLock.writeLock().lock();
			itemTopK = this.topK.get(item);
			if (itemTopK == null) {
				itemTopK = new PatternWithFreq[this.k];
				itemTopK[0] = new PatternWithFreq(support, pattern);
				this.topK.put(item, itemTopK);
			}
			this.mapLock.writeLock().unlock();
			this.mapLock.readLock().lock();
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
					// find where the new pattern is going to be inserted in
					// the
					// sorted topk list
					int newPosition = k - 1;
					while (newPosition >= 1) {
						if (itemTopK[newPosition - 1].getSupportCount() < support) {
							newPosition--;
						} else {
							break;
						}
					}
					// make room for the new pattern, evicting the one at
					// the
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

	@Override
	public void collect(final int support, final int[] pattern) {
		this.mapLock.readLock().lock();
		super.collect(support, pattern);
		this.mapLock.readLock().unlock();
	}

	@Override
	public int explore(final int[] currentPattern, final int extension,
			final int[] sortedFreqItems, final TIntIntMap supportCounts,
			final TIntIntMap failedPPTests, final int previousItem,
			final int resultForPreviousItem) {
		this.mapLock.readLock().lock();
		int threshold = super.explore(currentPattern, extension,
				sortedFreqItems, supportCounts, failedPPTests);
		this.mapLock.readLock().unlock();
		return threshold;
	}

	public static void main(String[] args) {
		final PerItemTopKCollectorRWLock topk = new PerItemTopKCollectorRWLock(
				new StdOutCollector(), 3, true);
		topk.collect(10, new int[] { 3, 1, 2 });
		topk.collect(100, new int[] { 1 });
		topk.collect(30, new int[] { 1, 3 });
		topk.collect(20, new int[] { 2, 3 });
		topk.collect(50, new int[] { 1, 4 });
		topk.collect(60, new int[] { 5, 4 });
		topk.collect(70, new int[] { 6, 4 });
		topk.collect(50, new int[] { 0 });
		topk.collect(35, new int[] { 1, 0 });
		topk.collect(20, new int[] { 2, 0 });
		System.out.println(topk);
		topk.close();
	}
}
