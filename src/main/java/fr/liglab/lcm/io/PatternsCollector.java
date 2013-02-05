package fr.liglab.lcm.io;

public interface PatternsCollector {
	public void collect(int support, int[] pattern);
	
	/**
	 * Call this once mining has terminated.
	 * Behavior of the collect method is undefined once close() has been called
	 */
	public void close();
}
