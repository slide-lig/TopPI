package fr.liglab.mining.io;

import fr.liglab.mining.TopLCM;
import fr.liglab.mining.TopLCM.TopLCMCounters;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.internals.Selector;
import fr.liglab.mining.internals.TransactionReader;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a collector. As a (stateful) Selector it will limit exploration to
 * top-k-per-items patterns.
 * 
 * Thread-safe and initialized with known frequent items.
 * 
 * Note the collector will only consider items provided at instantiation ; any other 
 * will be silently ignored. This is useful when mining by groups.
 */
public class PerItemTopKCollector implements PatternsCollector {

	private final PatternsCollector decorated;
	protected final int k;
	protected final TIntObjectMap<PatternWithFreq[]> topK;
	
	/**
	 * When set to true, instead of outputting each item's top-k-patterns the program will only 
	 * give a single (fake) pattern per frequent item : the item itself, its patterns count (max=K), 
	 * its patterns' supports sum and its lowest pattern support. 
	 * Given support will be item's support count.
	 */
	protected boolean infoMode;
	
	protected boolean outputUniqueOnly;
	
	/**
	 * when provided (through readPerItemKFrom() ) the collector, before closing and outputting 
	 * collected patterns, will read the given file and restrict each item's topK to the given value.
	 * file format is simple ASCII (as input files) and should give, per line : ITEM_ID NB_PATTERNS_TO_KEEP
	 */
	protected String pathToPerItemK = null;

	public PerItemTopKCollector(final PatternsCollector follower, final int k, final int nbItems,
			final FrequentsIterator items) {

		this.decorated = follower;
		this.k = k;
		this.topK = new TIntObjectHashMap<PatternWithFreq[]>(nbItems);

		for (int item = items.next(); item != -1; item = items.next()) {
			this.topK.put(item, new PatternWithFreq[k]);
		}
	}
	
	public final PatternWithFreq preCollect(final int support, final int[] parentPattern, 
			final int extensionInternalID, final int extensionOriginalID) {
		PatternWithFreq placeholder = new PatternWithFreq(support, extensionInternalID);
		int nbRefs = 0;
		for (final int item : parentPattern) {
			if (insertPatternInTop(placeholder, item)) {
				nbRefs++;
			}
		}
		
		if (insertPatternInTop(placeholder, extensionOriginalID)) {
			nbRefs++;
		}
		
		placeholder.incrementRefCount(nbRefs);
		return placeholder;
	}
	
	public final void collect(final int support, final int[] pattern) {
		throw new IllegalArgumentException("Thou shall preCollect() and setPattern()");
	}
	
	/**
	 * @return true if the insertion actually happened
	 */
	public boolean insertPatternInTop(PatternWithFreq entry, int item) {
		PatternWithFreq[] itemTopK = this.topK.get(item);
		
		if (itemTopK != null) {
			synchronized (itemTopK) {
				return updateTop(entry, itemTopK);
			}
		}
		
		return false;
	}
	
	/**
	 * @return true if the insertion actually happened
	 */
	private boolean updateTop(PatternWithFreq entry, PatternWithFreq[] itemTopK) {
		final int support = entry.getSupportCount();
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
			itemTopK[newPosition] = entry;
			return true;
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
			
			// no, there is no double locking behind this: two threads may be ejecting the same 
			// pattern from two distinct topKlists
			itemTopK[this.k - 1].onEjection();
			
			// make room for the new pattern, evicting the one at the end
			for (int i = this.k - 1; i > newPosition; i--) {
				itemTopK[i] = itemTopK[i - 1];
			}
			// insert the new pattern where previously computed
			itemTopK[newPosition] = entry;
			return true;
		}
		// else not in top k for this item, do nothing
		return false;
	}

	public long close() {
		if (this.pathToPerItemK != null) {
			this.applyPerItemKRestriction();
		}
		
		if (this.infoMode) {
			this.collectItemStats();
		} else if (this.outputUniqueOnly) {
			this.outputUniquePatterns();
		} else {
			this.outputAll();
		}
		
		return this.decorated.close();
	}

	private void applyPerItemKRestriction() {
		FileReader reader = new FileReader(this.pathToPerItemK);
		
		while(reader.hasNext()) {
			TransactionReader line = reader.next();
			int itemID = line.next();
			int maxPatterns = line.next();
			
			if (maxPatterns < this.k) {
				PatternWithFreq[] patternWithFreqs = this.topK.get(itemID);
				for (int i = maxPatterns; i < patternWithFreqs.length && patternWithFreqs[i] != null; i++) {
					patternWithFreqs[i] = null;
				}
			}
		}
	}

	private void outputUniquePatterns() {
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
	}

	protected void outputAll() {
		for (final PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
			for (int i = 0; i < itemTopK.length; i++) {
				if (itemTopK[i] == null) {
					break;
				} else {
					this.decorated.collect(itemTopK[i].getSupportCount(), itemTopK[i].getPattern());
				}
			}
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
			int supportSum = 0;
			while (nbPatterns < this.k && itemTopK[nbPatterns] != null) {
				supportSum += itemTopK[nbPatterns].getSupportCount();
				nbPatterns++;
			}
			
			int lowestSupport = itemTopK[nbPatterns-1].getSupportCount();
			
			if (itemTopK[0] != null) {
				this.decorated.collect(itemTopK[0].getSupportCount(), new int[] {item, nbPatterns, supportSum, lowestSupport});
			}
		}
	}
	
	/**
	 * @return MAX_VALUE if item is unknown, -1 if item's top-K isn't full, or item's K-th itemset's support count
	 */
	protected int getBound(final int item) {
		final PatternWithFreq[] itemTopK = this.topK.get(item);
		if (itemTopK == null) {
			return Integer.MAX_VALUE;
		} else if (itemTopK[this.k - 1] == null) {
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
		protected final int supportCount;
		protected int[] pattern = null;
		public final int extension;
		private final AtomicInteger refCount = new AtomicInteger();

		public PatternWithFreq(final int supportCount, final int extension) {
			super();
			this.extension = extension;
			this.supportCount = supportCount;
		}

		public int getSupportCount() {
			return this.supportCount;
		}

		public int[] getPattern() {
			return this.pattern;
		}
		
		/**
		 * to be called upon validation
		 */
		public void setPattern(int[] p) {
			this.pattern = p;
		}
		
		public void incrementRefCount(int delta) {
			this.refCount.addAndGet(delta);
		}
		
		void onEjection() {
			if (this.pattern == null) {
				((TopLCM.TopLCMThread) Thread.currentThread()).counters[TopLCMCounters.EjectedPlaceholders.ordinal()]++;
			} else {
				((TopLCM.TopLCMThread) Thread.currentThread()).counters[TopLCMCounters.EjectedPatterns.ordinal()]++;
			}
			
			this.refCount.decrementAndGet();
		}
		
		public boolean isStillInTopKs() {
			return this.refCount.get() > 0;
		}
		@Override
		public String toString() {
			return "[supportCount=" + this.supportCount + ", pattern=" + Arrays.toString(this.pattern) + "]";
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(this.pattern);
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
			if (this.supportCount != other.supportCount)
				return false;
			if (!Arrays.equals(this.pattern, other.pattern))
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
		protected TopLCMCounters getCountersKey() {
			return TopLCMCounters.TopKRejections;
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

	public void setOutputUniqueOnly(boolean outputUniqueOnly) {
		this.outputUniqueOnly = outputUniqueOnly;
	}

	public void readPerItemKFrom(String path) {
		this.pathToPerItemK = path;
	}
}
