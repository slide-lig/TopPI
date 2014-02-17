package fr.liglab.mining.internals;


public interface FrequentsIterator {
	/**
	 * @return -1 when iterator is finished
	 */
	public int next();

	/**
	 * @return last value returned by next()
	 */
	public int peek();

	/**
	 * @return a higher bound on iterated items
	 */
	public int last();
}
