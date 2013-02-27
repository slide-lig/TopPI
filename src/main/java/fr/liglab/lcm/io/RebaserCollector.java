package fr.liglab.lcm.io;

import java.util.Arrays;

import fr.liglab.lcm.internals.RebasedDataset;

/**
 * a PatternsCollector decorator : it will transmit everything, 
 * except that all patterns are re-instanciated and translated according 
 * to the RebasedDataset provided at instanciation.
 */
public class RebaserCollector implements PatternsCollector {
	
	protected final int[] map;
	protected final PatternsCollector decorated;
	
	public RebaserCollector(PatternsCollector wrapped, RebasedDataset initalDataset) {
		this.map = initalDataset.getReverseMap();
		this.decorated = wrapped;
	}

	public void collect(final int support, final int[] pattern) {
		int[] rebased = Arrays.copyOf(pattern, pattern.length);
		
		for (int i = 0; i < pattern.length; i++) {
			rebased[i] = this.map[pattern[i]];
		}
		
		this.decorated.collect(support, rebased);
	}

	public void close() {
		this.decorated.close();
	}

}