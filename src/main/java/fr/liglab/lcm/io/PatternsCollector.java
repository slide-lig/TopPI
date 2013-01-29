package fr.liglab.lcm.io;

import fr.liglab.lcm.internals.Itemset;

public interface PatternsCollector {
	public void collect(Long support, Itemset pattern);
	
	/**
	 * Call this once mining has terminated.
	 * Behavior of the collect method is undefined once close() has been called
	 */
	public void close();
}
