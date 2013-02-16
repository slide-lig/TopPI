package fr.liglab.lcm.io;

import java.util.Arrays;

/**
 * Decorated another PatternsCollector : it will transmit everything, 
 * except that all patterns are re-instanciated and translated according 
 * to the map provided at instanciation.
 * 
 * It's made for items in [O;maxItem] so we're using a classic array as a map
 */
public class RebaserCollector implements PatternsCollector {
	
	protected final int[] map;
	protected final PatternsCollector decorated;
	
	public RebaserCollector(PatternsCollector wrapped, final int[] reverseMap) {
		this.map = reverseMap;
		this.decorated = wrapped;
	}

	public void collect(final int support, final int[] pattern) {
		int[] rebased = Arrays.copyOf(pattern, pattern.length);
		
		for (int i = 0; i < pattern.length; i++) {
			rebased[i] = map[pattern[i]];
		}
		
		decorated.collect(support, rebased);
	}

	public void close() {
		decorated.close();
	}

}
