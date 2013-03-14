package fr.liglab.lcm.io;

import fr.liglab.lcm.internals.Dataset;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class PerItemTopKCollectorThreadSafeInitialized extends
		PatternsCollector {
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
	private final PatternsCollector decorated;
	private final int k;
	private final TIntObjectMap<PatternWithFreq[]> topK;
	private final boolean outputEachPatternOnce;

	public PerItemTopKCollectorThreadSafeInitialized(
			final PatternsCollector decorated, Dataset dataset, final int k) {
		this(decorated, k, dataset, false);
	}

	public PerItemTopKCollectorThreadSafeInitialized(
			final PatternsCollector follower, final int k, Dataset dataset,
			final boolean outputEachPatternOnce) {
		this.decorated = follower;
		this.k = k;
		// we may want to hint a default size, it is at least the group size,
		// but in practice much bigger
		this.topK = new TIntObjectHashMap<PatternWithFreq[]>();
		this.outputEachPatternOnce = outputEachPatternOnce;
		this.init(dataset.getSupportCounts().keySet().iterator());
	}

	private void init(final TIntIterator items) {
		while (items.hasNext()) {
			this.topK.put(items.next(), new PatternWithFreq[k]);
		}
	}

	public void collect(final int support, final int[] pattern) {
		for (final int item : pattern) {
			PatternWithFreq[] itemTopK = this.topK.get(item);
			// first item in topk
			if (itemTopK == null) {
				throw new RuntimeException("item not initialied " + item);
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
	}

	public void close() {
		if (this.outputEachPatternOnce) {
			final Set<PatternWithFreq> dedup = new HashSet<PatternWithFreq>();
			for (final PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
				for (int i = 0; i < itemTopK.length; i++) {
					if (itemTopK[i] == null) {
						break;
					} else {
						if (dedup.add(itemTopK[i])) {
							this.decorated.collect(
									itemTopK[i].getSupportCount(),
									itemTopK[i].getPattern());
						}
					}
				}
			}
		} else {
			for (final PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
				for (int i = 0; i < itemTopK.length; i++) {
					if (itemTopK[i] == null) {
						break;
					} else {
						this.decorated.collect(itemTopK[i].getSupportCount(),
								itemTopK[i].getPattern());
					}
				}
			}
		}
		this.decorated.close();
	}

	/*
	 * @param currentPattern Pattern corresponding to the current dataset
	 * 
	 * @param extension Proposition of an item to extend the current pattern
	 * 
	 * @param sortedFreqItems array of remaining frequent items in the current
	 * data set (sorted in increasing order)
	 * 
	 * @param supportCounts Map giving the support for each item present in the
	 * current dataset
	 * 
	 * @return true if it is possible to generate patterns that make it into
	 * topK by exploring this extension
	 */
	// Assumes that patterns are extended with lower IDs
	// Also assumes that frequency test is already done
	@Override
	public int explore(final int[] currentPattern, final int extension,
			final int[] sortedFreqItems, final TIntIntMap supportCounts,
			final int previousItem, final int resultForPreviousItem) {
		if (currentPattern.length == 0) {
			return -1;
		}
		final int extensionSupport = supportCounts.get(extension);
		int threshold = Integer.MAX_VALUE;
		boolean shortcut = resultForPreviousItem > extensionSupport;
		if (shortcut) {
			threshold = Math.min(resultForPreviousItem, threshold);
		} else {
			for (int item : currentPattern) {
				final PatternWithFreq[] itemTopK = this.topK.get(item);
				// itemTopK == null should never happen in theory, as
				// currentPattern should be in there at least
				if (itemTopK == null
						|| itemTopK[this.k - 1] == null
						|| itemTopK[this.k - 1].getSupportCount() < extensionSupport) {
					return -1;
				} else {
					threshold = Math.min(threshold,
							itemTopK[this.k - 1].getSupportCount());
				}
			}
		}
		// check for extension
		final PatternWithFreq[] itemTopK = this.topK.get(extension);
		if (itemTopK == null || itemTopK[this.k - 1] == null
				|| itemTopK[this.k - 1].getSupportCount() < extensionSupport) {
			return -1;
		} else {
			threshold = Math.min(threshold,
					itemTopK[this.k - 1].getSupportCount());
		}
		int i = 0;
		if (shortcut) {
			i = Arrays.binarySearch(sortedFreqItems, previousItem);
			if (i < 0) {
				throw new RuntimeException(
						"previous item not in frequent items");
			}
			i++;
		}
		// check for items < extension
		// keep in mind that their max support will be the min of their own
		// support in current dataset and support of the extension
		for (; i < sortedFreqItems.length; i++) {
			int item = sortedFreqItems[i];
			if (item >= extension) {
				break;
			}

			final PatternWithFreq[] potentialExtensionTopK = this.topK
					.get(item);

			if (potentialExtensionTopK == null
					|| potentialExtensionTopK[this.k - 1] == null
					|| potentialExtensionTopK[this.k - 1].getSupportCount() < Math
							.min(extensionSupport, supportCounts.get(item))) {
				return -1;
			} else {
				threshold = Math.min(threshold,
						potentialExtensionTopK[this.k - 1].getSupportCount());
			}
		}
		return threshold;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		this.topK.forEachEntry(new TIntObjectProcedure<PatternWithFreq[]>() {

			public boolean execute(int key, PatternWithFreq[] value) {
				sb.append("item " + key + " patterns");
				for (int i = 0; i < value.length; i++) {
					if (value[i] == null) {
						break;
					} else {
						sb.append(" " + value[i]);
					}
				}
				sb.append("\n");
				return true;
			}
		});
		return sb.toString();
	}

	private static final class PatternWithFreq {
		private final int supportCount;
		private final int[] pattern;

		public PatternWithFreq(final int supportCount, final int[] pattern) {
			super();
			this.supportCount = supportCount;
			this.pattern = pattern;
		}

		public final int getSupportCount() {
			return supportCount;
		}

		public final int[] getPattern() {
			return pattern;
		}

		@Override
		public String toString() {
			return "[supportCount=" + supportCount + ", pattern="
					+ Arrays.toString(pattern) + "]";
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(pattern);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PatternWithFreq other = (PatternWithFreq) obj;
			if (supportCount != other.supportCount)
				return false;
			if (!Arrays.equals(pattern, other.pattern))
				return false;
			return true;
		}

	}
}
