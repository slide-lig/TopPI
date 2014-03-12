package fr.liglab.mining.io;

import fr.liglab.mining.CountersHandler;
import fr.liglab.mining.CountersHandler.TopLCMCounters;
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

/**
 * Wraps a collector. As a (stateful) Selector it will limit exploration to
 * top-k-per-items patterns.
 * 
 * Thread-safe and initialized with known frequent items.
 * 
 * Note the collector will only consider items provided at instantiation ; any
 * other will be silently ignored. This is useful when mining by groups.
 */
public class PerItemTopKCollector implements PatternsCollector {

	private final PatternsCollector decorated;
	protected final int k;
	protected final TIntObjectMap<PatternWithFreq[]> topK;

	/**
	 * When set to true, instead of outputting each item's top-k-patterns the
	 * program will only give a single (fake) pattern per frequent item : the
	 * item itself, its patterns count (max=K), its patterns' supports sum and
	 * its lowest pattern support. Given support will be item's support count.
	 */
	protected boolean infoMode;

	protected boolean outputUniqueOnly;

	/**
	 * when provided (through readPerItemKFrom() ) the collector, before closing
	 * and outputting collected patterns, will read the given file and restrict
	 * each item's topK to the given value. file format is simple ASCII (as
	 * input files) and should give, per line : ITEM_ID NB_PATTERNS_TO_KEEP
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

	public final void collect(final int support, final int[] pattern) {
		this.collect(support, pattern, true);
	}

	public final void collect(final int support, final int[] pattern, boolean closed) {
		PatternWithFreq p = new PatternWithFreq(support, pattern, closed);
		for (final int item : pattern) {
			insertPatternInTop(p, item);
		}
	}

	public final int collectForItem(final int support, final int[] parentPattern, final int item) {
		int prevSup = this.getBound(item);
		if (support >= prevSup) {
			// deliberately not adding item to array
			// the pattern is not closed anyway, so would cost and not help
			PatternWithFreq p = new PatternWithFreq(support, parentPattern, false);
			if (insertPatternInTop(p, item)) {
				return this.getBound(item);
			} else {
				return prevSup;
			}
		} else {
			return prevSup;
		}
	}

	/**
	 * @return true if the insertion actually happened
	 */
	public boolean insertPatternInTop(PatternWithFreq entry, int item) {
		PatternWithFreq[] itemTopK = this.topK.get(item);

		if (itemTopK != null) {
			synchronized (itemTopK) {
				if (updateTop(entry, itemTopK, item)) {
					entry.incrementRefCount(1);
					return true;
				} else {
					return false;
				}
			}
		}

		return false;
	}

	/**
	 * @return true if the insertion actually happened
	 */
	private boolean updateTop(PatternWithFreq entry, PatternWithFreq[] itemTopK, int item) {
		// ordering is first based on support and then on length
		// careful! if adding a non closed we want to find other non closed even
		// if shorter length
		final int support = entry.getSupportCount();
		int insertPos = 0;
		int evictedPos = -1;
		if (entry.isClosed()) {
			for (int i = this.k - 1; i >= 0; i--) {
				if (itemTopK[i] == null) {
				} else if (itemTopK[i].getSupportCount() < support) {
				} else if (itemTopK[i].getSupportCount() > support) {
					insertPos = i + 1;
					break;
				} else if (itemTopK[i].isClosed()) {
					if (itemTopK[i].getPattern().length < entry.getPattern().length) {
						insertPos = i + 1;
						break;
					} else if (itemTopK[i].getPattern().length == entry.getPattern().length) {
						if (Arrays.equals(entry.getPattern(), itemTopK[i].getPattern())) {
							// closed pattern already seen (early output),
							// reject
							return false;
						}
					}
				} else {
					// non closed pattern of same support
					// there can only be 1 now so 1 variable for evicted is
					// enough
					if (itemTopK[i].getPattern().length <= entry.getPattern().length) {
						if (subPattern(itemTopK[i].getPattern(), entry.getPattern())) {
							evictedPos = i;
						}
					}
				}
			}
		} else {
			for (int i = this.k - 1; i >= 0; i--) {
				if (itemTopK[i] == null) {
				} else if (itemTopK[i].getSupportCount() < support) {
				} else if (itemTopK[i].getSupportCount() > support) {
					if (insertPos == 0) {
						insertPos = i + 1;
					}
					break;
				} else if (itemTopK[i].isClosed()) {
					// closed pattern of same support, must insert before
					if (insertPos == 0) {
						insertPos = i + 1;
					}
					if (itemTopK[i].getPattern().length >= entry.getPattern().length) {
						if (subPattern(entry.getPattern(), itemTopK[i].getPattern())) {
							// non closed pattern contained in a known closed
							// one,
							// reject
							return false;
						}
					} else {
						break;
					}
				} else {
					// we do not accept two non closed patterns of same support
					return false;
				}
			}
		}
		if (insertPos == this.k) {
			return false;
		}
		if (evictedPos == -1) {
			if (itemTopK[this.k - 1] != null) {
				itemTopK[this.k - 1].onEjection();
			}
			if (insertPos != this.k - 1) {
				System.arraycopy(itemTopK, insertPos, itemTopK, insertPos + 1, this.k - insertPos - 1);
			}
			itemTopK[insertPos] = entry;
			return true;
		} else {
			itemTopK[evictedPos].onEjection();
			if (insertPos != evictedPos) {
				if (insertPos > evictedPos) {
					System.arraycopy(itemTopK, evictedPos + 1, itemTopK, evictedPos, insertPos - evictedPos);
				} else {
					System.arraycopy(itemTopK, insertPos + 1, itemTopK, insertPos, evictedPos - insertPos);
				}
			}
			itemTopK[insertPos] = entry;
			return true;
		}
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

		while (reader.hasNext()) {
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

	public void outputAll() {
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

			int lowestSupport = itemTopK[nbPatterns - 1].getSupportCount();

			if (itemTopK[0] != null) {
				this.decorated.collect(itemTopK[0].getSupportCount(), new int[] { item, nbPatterns, supportSum,
						lowestSupport });
			}
		}
	}

	/**
	 * @return MAX_VALUE if item is unknown, -1 if item's top-K isn't full, or
	 *         item's K-th itemset's support count
	 */
	public int getBound(final int item) {
		PatternWithFreq p = this.topK.get(item)[this.k - 1];
		if (p == null) {
			return -1;
		} else if (p.isClosed()) {
			return p.getSupportCount() + 1;
		} else {
			return p.getSupportCount();
		}
	}

	public TIntIntMap getTopKBounds() {
		final TIntIntMap output = new TIntIntHashMap(this.topK.size());
		this.topK.forEachEntry(new TIntObjectProcedure<PatternWithFreq[]>() {

			@Override
			public boolean execute(int k, PatternWithFreq[] v) {
				int s;
				PatternWithFreq p = v[v.length - 1];
				if (p == null) {
					s = -1;
				} else if (p.isClosed()) {
					s = p.getSupportCount() + 1;
				} else {
					s = p.getSupportCount();
				}
				output.put(k, s);
				return true;
			}
		});
		return output;
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

	// assumes sorted
	private static boolean subPattern(int[] p1, int[] p2) {
		// if (p1.length > p2.length) {
		// throw new RuntimeException("should not happen");
		// }
		int skipsAllowed = p2.length - p1.length;
		int p1Index = 0;
		int p2Index = 0;
		while (p1Index < p1.length) {
			if (p1[p1Index] == p2[p2Index]) {
				p1Index++;
				p2Index++;
			} else {
				skipsAllowed--;
				if (skipsAllowed < 0) {
					return false;
				} else {
					p2Index++;
				}
			}
		}
		return true;
	}

	public static final class PatternWithFreq {

		protected final int supportCount;
		protected int[] pattern = null;
		private int nbRefs = 0;
		protected boolean closed = true;

		public PatternWithFreq(final int supportCount) {
			super();
			this.supportCount = supportCount;
		}

		public PatternWithFreq(final int supportCount, final int[] pattern) {
			super();
			this.supportCount = supportCount;
			this.pattern = pattern;
			Arrays.sort(this.pattern);
		}

		public PatternWithFreq(final int supportCount, final int[] pattern, boolean closed) {
			this(supportCount, pattern);
			this.closed = closed;
		}

		public final boolean isClosed() {
			return closed;
		}

		public final void setClosed(boolean closed) {
			this.closed = closed;
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
			Arrays.sort(this.pattern);
		}

		public synchronized void onEjection() {
			this.nbRefs--;
			if (nbRefs == 0) {
				CountersHandler.increment((this.pattern == null) ? TopLCMCounters.EjectedPlaceholders
						: TopLCMCounters.EjectedPatterns);
			}
		}

		public boolean isStillInTopKs() {
			return this.nbRefs > 0;
		}

		public synchronized void incrementRefCount(int d) {
			this.nbRefs += d;
		}

		@Override
		public String toString() {
			return "[supportCount=" + this.supportCount + ", pattern=" + Arrays.toString(this.pattern) + ", closed="
					+ this.closed + "]";
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
				for (int i : state.counters.pattern) {
					int bound = getBound(i);
					if (bound <= extensionSupport) {
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
				if (bound <= Math.min(extensionSupport, supports[i])) {
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

	public int getK() {
		return this.k;
	}

	public static void main(String[] args) {
		PerItemTopKCollector coll = new PerItemTopKCollector(null, 5, 1, new FrequentsIterator() {
			boolean done = false;

			@Override
			public int peek() {
				return 1;
			}

			@Override
			public int next() {
				if (!done) {
					done = true;
					return 1;
				} else {
					return -1;
				}
			}

			@Override
			public int last() {
				return 1;
			}
		});

		coll.insertPatternInTop(new PatternWithFreq(100, new int[] { 1, 24 }, true), 1);
		coll.insertPatternInTop(new PatternWithFreq(100, new int[] { 2, 57, 78 }, true), 1);
		coll.insertPatternInTop(new PatternWithFreq(110, new int[] { 3 }, false), 1);
		coll.insertPatternInTop(new PatternWithFreq(100, new int[] { 4 }, false), 1);
		System.out.println(coll);
		System.out.println(coll.getBound(1));
		coll.insertPatternInTop(new PatternWithFreq(100, new int[] { 5 }, true), 1);
		System.out.println(coll);
		System.out.println(coll.getBound(1));
		coll.insertPatternInTop(new PatternWithFreq(100, new int[] { 6 }, true), 1);
		System.out.println(coll);
		System.out.println(coll.getBound(1));
	}
}
