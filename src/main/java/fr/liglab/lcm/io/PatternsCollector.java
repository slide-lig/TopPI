package fr.liglab.lcm.io;


public interface PatternsCollector {
	public abstract void collect(final int support, final int[] pattern);

	/**
	 * Call this once mining has terminated. Behavior of the collect method is
	 * undefined once close() has been called
	 * 
	 * @return outputted pattern count
	 */
	public abstract long close();
}
