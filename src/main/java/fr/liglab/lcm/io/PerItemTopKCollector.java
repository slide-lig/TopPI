package fr.liglab.lcm.io;

import fr.liglab.lcm.internals.FrequentsIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a collector and will limit exploration to top-k-per-items patterns
 * 
 * Thread-safe and initialized with known frequent items.
 */
public class PerItemTopKCollector extends PatternsCollector {
	
	private final PatternsCollector decorated;
	protected final int k;
	protected final TIntObjectMap<PatternWithFreq[]> topK;
	
	public PerItemTopKCollector(final PatternsCollector follower, final int k, 
			final int nbItems, final FrequentsIterator items) {
		
		this.decorated = follower;
		this.k = k;
		this.topK = new TIntObjectHashMap<PatternWithFreq[]>(nbItems);
		
		for (int item = items.next(); item != -1; item = items.next()) {
			this.topK.put(item, new PatternWithFreq[k]);
		}
	}

	public void collect(final int support, final int[] pattern) {
		for (final int item : pattern) {
			insertPatternInTop(support, pattern, item);
		}
	}
	
	protected void insertPatternInTop(final int support, final int[] pattern, int item) {
		PatternWithFreq[] itemTopK = this.topK.get(item);
		
		if (itemTopK == null) {
			throw new RuntimeException("item not initialized " + item);
		} else {
			synchronized (itemTopK) {
				updateTop(support, pattern, itemTopK);
			}
		}
	}

	private void updateTop(final int support, final int[] pattern, PatternWithFreq[] itemTopK) {
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
			itemTopK[newPosition] = new PatternWithFreq(support, pattern);
		} else
		// the support of the new pattern is higher than the kth previously
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
			// make room for the new pattern, evicting the one at the end
			for (int i = this.k - 1; i > newPosition; i--) {
				itemTopK[i] = itemTopK[i - 1];
			}
			// insert the new pattern where previously computed
			itemTopK[newPosition] = new PatternWithFreq(support, pattern);
		}
		// else not in top k for this item, do nothing
	}

	public long close() {
		final Set<PatternWithFreq> dedup = new HashSet<PatternWithFreq>();
		for (final PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
			for (int i = 0; i < itemTopK.length; i++) {
				if (itemTopK[i] == null) {
					break;
				} else {
					if (dedup.add(itemTopK[i])) {
						this.decorated.collect(itemTopK[i].getSupportCount(), itemTopK[i].getPattern());
					}
				}
			}
		}
		
		return this.decorated.close();
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
	 * @param failedPPTests Map in which items previously rejected by PPTest are
	 * associated to their greatest first parent
	 * 
	 * @return true if it is possible to generate patterns that make it into
	 * topK by exploring this extension
	 */
	// Assumes that patterns are extended with lower IDs
	// Also assumes that frequency test is already done
	@Override
	public int explore(final int[] currentPattern, final int extension, final int[] sortedFreqItems,
			final TIntIntMap supportCounts, final TIntIntMap failedPPTests, final int previousItem,
			final int resultForPreviousItem) {

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
				int itemTest = this.checkExploreInCurrentPattern(item, extensionSupport);
				if (itemTest == -1) {
					return -1;
				} else {
					threshold = Math.min(threshold, itemTest);
				}
			}
		}
		// check for extension
		{
			int itemTest = this.checkExploreInCurrentPattern(extension, extensionSupport);
			if (itemTest == -1) {
				return -1;
			} else {
				threshold = Math.min(threshold, itemTest);
			}
		}
		int i = 0;
		if (shortcut) {
			i = Arrays.binarySearch(sortedFreqItems, previousItem);
			if (i < 0) {
				throw new RuntimeException("previous item not in frequent items");
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
			int itemTest = this.checkExploreOtherItem(item, supportCounts.get(item), extension, extensionSupport,
					failedPPTests);
			if (itemTest == -1) {
				return -1;
			} else {
				threshold = Math.min(threshold, itemTest);
			}
		}
		return threshold;
	}

	protected int checkExploreInCurrentPattern(final int item, final int itemSupport) {
		final PatternWithFreq[] itemTopK = this.topK.get(item);
		// itemTopK == null should never happen in theory, as
		// currentPattern should be in there at least
		if (itemTopK == null || itemTopK[this.k - 1] == null || itemTopK[this.k - 1].getSupportCount() < itemSupport) {
			return -1;
		} else {
			return itemTopK[this.k - 1].getSupportCount();
		}
	}

	protected int checkExploreOtherItem(final int item, final int itemSupport, final int extension,
			final int extensionSupport, final TIntIntMap failedPPTests) {
		final PatternWithFreq[] potentialExtensionTopK = this.topK.get(item);

		if (potentialExtensionTopK == null || potentialExtensionTopK[this.k - 1] == null
				|| potentialExtensionTopK[this.k - 1].getSupportCount() < Math.min(extensionSupport, itemSupport)) {

			if (failedPPTests == null || failedPPTests.get(item) <= extension) {
				return -1;
			} else {
				return Integer.MAX_VALUE;
			}

		} else {
			return potentialExtensionTopK[this.k - 1].getSupportCount();
		}
	}
	
	public TIntIntMap getTopKBounds() {
		final TIntIntHashMap bounds = new TIntIntHashMap(this.topK.size());
		TIntObjectIterator<PatternWithFreq[]> it = this.topK.iterator();
		while (it.hasNext()) {
			it.advance();
			int bound = 0;
			if (it.value()[this.k - 1] != null) {
				bound = it.value()[this.k - 1].supportCount;
			}
			bounds.put(it.key(), bound);
		}
		return bounds;
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
	
	
	

	public static final class PatternWithFreq {
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
			return "[supportCount=" + supportCount + ", pattern=" + Arrays.toString(pattern) + "]";
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
