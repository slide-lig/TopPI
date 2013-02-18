package fr.liglab.lcm.io;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

public class PerItemTopKCollector implements PatternsCollector {
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
	private final PatternsCollector follower;
	private final int k;
	private final TIntObjectMap<PatternWithFreq[]> topK;
	private final boolean outputEachPatternOnce;

	public PerItemTopKCollector(PatternsCollector follower, int k) {
		this(follower, k, false);
	}

	public PerItemTopKCollector(PatternsCollector follower, int k,
			boolean outputEachPatternOnce) {
		this.follower = follower;
		this.k = k;
		// we may want to hint a default size, it is at least the group size,
		// but in practice much bigger
		this.topK = new TIntObjectHashMap<PatternWithFreq[]>();
		this.outputEachPatternOnce = outputEachPatternOnce;
	}

	public void collect(final int support, final int[] pattern) {
		for (final int item : pattern) {
			PatternWithFreq[] itemTopK = this.topK.get(item);
			if (itemTopK == null) {
				itemTopK = new PatternWithFreq[this.k];
				this.topK.put(item, itemTopK);
			}
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
	}

	public void close() {
		if (this.outputEachPatternOnce) {
			Set<PatternWithFreq> dedup = new HashSet<>();
			for (PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
				for (int i = 0; i < itemTopK.length; i++) {
					if (itemTopK[i] == null) {
						break;
					} else {
						if (dedup.add(itemTopK[i])) {
							this.follower.collect(
									itemTopK[i].getSupportCount(),
									itemTopK[i].getPattern());
						}
					}
				}
			}
		} else {
			for (PatternWithFreq[] itemTopK : this.topK.valueCollection()) {
				for (int i = 0; i < itemTopK.length; i++) {
					if (itemTopK[i] == null) {
						break;
					} else {
						this.follower.collect(itemTopK[i].getSupportCount(),
								itemTopK[i].getPattern());
					}
				}
			}
		}
		this.follower.close();
	}

	/*
	 * @param currentPattern Pattern corresponding to the current dataset
	 * 
	 * @param currentSupport Support of currentPattern
	 * 
	 * @param extension Proposition of an item to extend the current pattern
	 * 
	 * @param support Map giving the support for each item present in the
	 * current dataset
	 * 
	 * @return true if it is possible to generate patterns that make it into
	 * topK by exploring this extension
	 */
	// Assumes that patterns are extended with lower IDs
	// Also assumes that frequency test is already done
	public boolean explore(int[] currentPattern, int currentSupport,
			int extension, SortedMap<Integer, Integer> support) {
		if (currentPattern.length == 0) {
			return true;
		}
		// start by checking the topk of items already in the pattern
		for (int item : currentPattern) {
			PatternWithFreq[] itemTopK = this.topK.get(item);
			// itemTopK == null should never happen in theory, as
			// currentPattern should be in there at least
			if (itemTopK == null || itemTopK[this.k - 1] == null
					|| itemTopK[this.k - 1].getSupportCount() < currentSupport) {
				return true;
			}
		}
		// check for extension
		int extensionSupport = support.get(extension);
		PatternWithFreq[] itemTopK = this.topK.get(extension);
		if (itemTopK == null || itemTopK[this.k - 1] == null
				|| itemTopK[this.k - 1].getSupportCount() < extensionSupport) {
			return true;
		}
		// check for items < extension
		// keep in mind that their max support will be the min of their own
		// support in current dataset and support of the extension
		for (Entry<Integer, Integer> en : support.headMap(extension).entrySet()) {
			itemTopK = this.topK.get(en.getKey());
			if (itemTopK == null
					|| itemTopK[this.k - 1] == null
					|| itemTopK[this.k - 1].getSupportCount() < Math.min(
							extensionSupport, en.getValue())) {
				return true;
			}
		}
		return false;
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

	private static class PatternWithFreq {
		private final int supportCount;
		private final int[] pattern;

		public PatternWithFreq(int supportCount, int[] pattern) {
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

	public static void main(String[] args) {
		PerItemTopKCollector topk = new PerItemTopKCollector(
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
