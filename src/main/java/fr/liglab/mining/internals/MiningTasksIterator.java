package fr.liglab.mining.internals;

import fr.liglab.mining.internals.ExplorationStep.MiningTask;
import fr.liglab.mining.io.PerItemTopKCollector.PatternWithFreq;

public interface MiningTasksIterator {
	/**
	 * @return -1 when iterator is finished
	 */
	public MiningTask next(ExplorationStep expStep);

	/**
	 * @return last value returned by next()
	 */
	public int peek();

	/**
	 * @return a higher bound on iterated items
	 */
	public int last();

	public void pushFutureWork(PatternWithFreq p);
}
