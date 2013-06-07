package fr.liglab.lcm.io;


public interface PatternsCollector {
	public void collect(final int support, final int[] pattern);

	/**
	 * Call this once mining has terminated. Behavior of the collect method is
	 * undefined once close() has been called
	 * 
	 * @return outputted pattern count
	 */
	public long close();
	
	/**
	 * It is safer to get this value once close() has been called.
	 * @return average length among outputted patterns
	 */
	public int getAveragePatternLength();
}
