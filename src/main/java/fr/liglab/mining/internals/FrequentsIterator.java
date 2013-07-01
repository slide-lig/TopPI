package fr.liglab.mining.internals;

public interface FrequentsIterator {
	/**
	 * @return -1 when iterator is finished
	 */
	public int next();
	
	/**
	 * @return last value returned by next() - trust this only when you're they only thread 
	 * accessing the iterator.
	 */
	public int peek();
	
	/**
	 * @return a higher bound on iterated items
	 */
	public int last();
	
	/**
	 * invoke this if the iterator should change its interval - be sure you're the only thread doing this
	 */
	public void reset(int from, int to);
}
