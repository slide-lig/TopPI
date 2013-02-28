package fr.liglab.lcm.io;

import gnu.trove.map.TIntIntMap;

public abstract class PatternsCollector {
	public abstract void collect(final int support, final int[] pattern);
	
	/**
	 * Call this once mining has terminated.
	 * Behavior of the collect method is undefined once close() has been called
	 */
	public abstract void close();
	
	/*
	 * @param currentPattern Pattern corresponding to the current dataset
	 * 
	 * @param currentSupport Support of currentPattern
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
	public boolean explore(final int[] currentPattern,
			final int currentSupport, final int extension,
			final int[] sortedFreqItems,
			final TIntIntMap supportCounts) {
		return true;
	}
}
