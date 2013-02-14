package fr.liglab.lcm.io;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

import java.util.Arrays;

public class PerItemTopKCollector implements PatternsCollector {
	/*
	 * If we go for threads, how do we make this threadsafe ? If this is not a
	 * bottleneck, synchronized on collect is ok Else, if we want parallelism,
	 * we need to take a lock when dealing with an item We can use a collection
	 * of locks and a hash of the item to do that safely, be careful with
	 * insertions in the map though
	 */
	private final PatternsCollector follower;
	private final int k;
	private final TIntObjectMap<PatternWithFreq[]> topK;

	public PerItemTopKCollector(PatternsCollector follower, int k) {
		this.follower = follower;
		this.k = k;
		// we may want to hint a default size, it is at least the group size,
		// but in practice much bigger
		this.topK = new TIntObjectHashMap<PatternWithFreq[]>();
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
		// output all patterns of top-k, generating multiple times the same
		// pattern if in different top-k
		// TODO output each pattern only once ? do we want this ? maybe not for
		// the MR version
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
		this.follower.close();
	}

	// given that I am going to explore a branch of patterns where the current
	// pattern is currentPattern and the maximum support maxSupport, is it
	// possible that I produce patterns that make it into the top-k of any item
	// ?
	// Assumes that the lowest ID item is at the end of the pattern, which
	// should be the case
	// Also assumes that minimum item ID is 0
	public boolean explore(int[] currentPattern, int maxSupport) {
		if (currentPattern.length == 0) {
			return true;
		}
		// start by checking the topk of items already in the pattern
		for (int item : currentPattern) {
			PatternWithFreq[] itemTopK = this.topK.get(item);
			if (itemTopK == null || itemTopK[this.k - 1] == null
					|| itemTopK[this.k - 1].getSupportCount() < maxSupport) {
				return true;
			}
		}
		for (int item = currentPattern[currentPattern.length - 1]; item >= 0; item--) {
			PatternWithFreq[] itemTopK = this.topK.get(item);
			if (itemTopK == null || itemTopK[this.k - 1] == null
					|| itemTopK[this.k - 1].getSupportCount() < maxSupport) {
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

	}

	public static void main(String[] args) {
		PerItemTopKCollector topk = new PerItemTopKCollector(
				new StdOutCollector(), 3);
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
		System.out.println(topk.explore(new int[] { 2, 1 }, 11));
		System.out.println(topk.explore(new int[] { 2, 1 }, 10));
		System.out.println(topk.explore(new int[] { 1 }, 9));
		System.out.println(topk.explore(new int[] { 4 }, 11));
		topk.close();
	}
}
