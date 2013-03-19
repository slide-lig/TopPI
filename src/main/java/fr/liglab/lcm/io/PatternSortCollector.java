package fr.liglab.lcm.io;

import java.util.Arrays;

/**
 * a PatternsCollector decorator : it will sort items in transactions before
 * transmitting them to the enclosed PatternsCollector
 */
public class PatternSortCollector extends PatternsCollector {

	protected final PatternsCollector decorated;

	public PatternSortCollector(PatternsCollector wrapped) {
		this.decorated = wrapped;
	}

	public void collect(final int support, final int[] pattern) {
		int[] sorted = Arrays.copyOf(pattern, pattern.length);
		Arrays.sort(sorted);
		this.decorated.collect(support, sorted);
	}

	public long close() {
		return this.decorated.close();
	}

}
