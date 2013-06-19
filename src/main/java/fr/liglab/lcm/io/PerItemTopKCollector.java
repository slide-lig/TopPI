package fr.liglab.lcm.io;

import fr.liglab.lcm.PLCM.PLCMCounters;
import fr.liglab.lcm.internals.ExplorationStep;
import fr.liglab.lcm.internals.FrequentsIterator;
import fr.liglab.lcm.internals.Selector;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.Arrays;

/**
 * Wraps a collector. As a (stateful) Selector it will limit exploration to
 * top-k-per-items patterns.
 * 
 * Thread-safe and initialized with known frequent items.
 */
public class PerItemTopKCollector implements PatternsCollector {

	private final PatternsCollector decorated;
	protected final int k;
	protected final TIntObjectMap<PatternWithFreq[]> topK;
	
	/**
	 * When set to true, instead of outputting each item's top-k-patterns the program will only 
	 * give a single (fake) pattern per frequent item : the item itself and its patterns count 
	 * (max=k) - given support will be item's support count.
	 */
	protected boolean infoMode;
	
	/**
	 * When set to true, instead of outputting each item's top-k-patterns the program will only 
	 * group frequent items by support count, and output for each distinct support value S a single 
	 * pattern containing a single integer ; the average support count of patterns for items of frequency S
	 */
	protected boolean supportInfoMode;

	public PerItemTopKCollector(final PatternsCollector follower, final int k, final int nbItems,
			final FrequentsIterator items) {

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
		// we do not want deduplication anymore
		// final Set<PatternWithFreq> dedup = new HashSet<PatternWithFreq>();
		
		if (this.infoMode) {
			this.collectItemStats();
		} else if (this.supportInfoMode) {
			this.collectPatternSupportsStats();
		} else {
			for (final PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
				for (int i = 0; i < itemTopK.length; i++) {
					if (itemTopK[i] == null) {
						break;
					} else {
						// if (dedup.add(itemTopK[i])) {
						this.decorated.collect(itemTopK[i].getSupportCount(), itemTopK[i].getPattern());
						// }
					}
				}
			}
		}
		
		return this.decorated.close();
	}

	private void collectPatternSupportsStats() {
		TIntObjectHashMap<TIntArrayList> patternsSupportByItemSupport = new TIntObjectHashMap<TIntArrayList>();
		TIntObjectIterator<PatternWithFreq[]> iterator = this.topK.iterator();
		
		while (iterator.hasNext()) {
			iterator.advance();
			
			PatternWithFreq[] itemTopK = iterator.value();
			
			/// hack hack hack !
			int itemSupport = itemTopK[0].supportCount;
			
			TIntArrayList patternSupports;
			
			if (patternsSupportByItemSupport.containsKey(itemSupport)) {
				patternSupports = patternsSupportByItemSupport.get(itemSupport);
			} else {
				patternSupports = new TIntArrayList();
				patternsSupportByItemSupport.put(itemSupport, patternSupports);
			}
			
			for (int i = 0;  i < itemTopK.length && itemTopK[i] != null; i++) {
				PatternWithFreq patternWithFreq = itemTopK[i];
				patternSupports.add(patternWithFreq.supportCount);
			}
		}
		
		int[] itemSupports = patternsSupportByItemSupport.keys();
		Arrays.sort(itemSupports);
		for (int i = 0; i < itemSupports.length; i++) {
			TIntArrayList supports = patternsSupportByItemSupport.get(itemSupports[i]);
			this.decorated.collect(itemSupports[i], new int[] {supports.sum()/supports.size()});
		}
	}

	/**
	 * @see infoMode
	 */
	protected void collectItemStats() {
		TIntObjectIterator<PatternWithFreq[]> iterator = this.topK.iterator();
		
		while (iterator.hasNext()) {
			iterator.advance();
			
			int item = iterator.key();
			PatternWithFreq[] itemTopK = iterator.value();
			
			int nbPatterns = 0;
			while (nbPatterns < this.k && itemTopK[nbPatterns] != null) {
				nbPatterns++;
			}
			
			if (itemTopK[0] != null) {
				this.decorated.collect(itemTopK[0].getSupportCount(), new int[] {item, nbPatterns});
			}
		}
	}

	protected int getBound(final int item) {
		final PatternWithFreq[] itemTopK = this.topK.get(item);
		// itemTopK == null should never happen in theory, as
		// currentPattern should be in there at least
		if (itemTopK == null || itemTopK[this.k - 1] == null) {
			return -1;
		} else {
			return itemTopK[this.k - 1].getSupportCount();
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

	public Selector asSelector() {
		return new ExplorationLimiter(null);
	}

	protected final class ExplorationLimiter extends Selector {

		private int previousItem = -1;
		private int previousResult = -1;
		private int validUntuil = Integer.MAX_VALUE;

		ExplorationLimiter(Selector follower) {
			super(follower);
		}

		@Override
		protected PLCMCounters getCountersKey() {
			return PLCMCounters.TopKRejections;
		}

		private synchronized void updatePrevious(final int i, final int r, int v) {
			if (i > this.previousItem) {
				this.previousItem = i;
				this.previousResult = r;
				this.validUntuil = v;
			}
		}

		@Override
		protected boolean allowExploration(int extension, ExplorationStep state) throws WrongFirstParentException {

			int localPreviousItem, localPreviousResult, localValidUntil;
			synchronized (this) {
				localPreviousItem = this.previousItem;
				localPreviousResult = this.previousResult;
				localValidUntil = this.validUntuil;
			}

			int[] reverseRenaming = state.counters.getReverseRenaming();
			int[] supports = state.counters.supportCounts;
			int extensionSupport = supports[extension];
			final int maxCandidate = state.counters.getMaxCandidate();

			boolean shortcut = localValidUntil > extension && localPreviousResult >= extensionSupport;

			if (getBound(reverseRenaming[extension]) < extensionSupport) {
				return true;
			}

			if (!shortcut) {
				localPreviousResult = Integer.MAX_VALUE;
				localValidUntil = Integer.MAX_VALUE;
				for (int i : state.pattern) {
					int bound = getBound(i);
					if (bound < extensionSupport) {
						return true;
					}
					localPreviousResult = Math.min(localPreviousResult, bound);
				}
			}

			FrequentsIterator it;
			if (shortcut) {
				it = state.counters.getLocalFrequentsIterator(localPreviousItem + 1, extension);
			} else {
				it = state.counters.getLocalFrequentsIterator(0, extension);
			}

			for (int i = it.next(); i >= 0; i = it.next()) {
				final int bound = getBound(reverseRenaming[i]);
				if (bound < Math.min(extensionSupport, supports[i])) {
					int firstParent = state.getFailedFPTest(i);

					if (firstParent <= extension) {
						this.updatePrevious(localPreviousItem, localPreviousResult, localValidUntil);
						return true;
					} else if (firstParent < maxCandidate) {
						localValidUntil = Math.min(localValidUntil, firstParent);
					}
				} else {
					localPreviousItem = i;
					localPreviousResult = Math.min(localPreviousResult, bound);
				}
			}

			this.updatePrevious(localPreviousItem, localPreviousResult, localValidUntil);

			return false;
		}

		@Override
		protected Selector copy(Selector newNext) {
			return new ExplorationLimiter(newNext);
		}
	}

	public int getAveragePatternLength() {
		return this.decorated.getAveragePatternLength();
	}

	public void setInfoMode(boolean outputInfoOnly) {
		this.infoMode = outputInfoOnly;
	}

	public void setSupportInfoMode(boolean outputSupportInfoOnly) {
		this.supportInfoMode = outputSupportInfoOnly;
	}
}
