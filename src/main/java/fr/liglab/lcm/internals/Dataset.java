package fr.liglab.lcm.internals;

import java.util.Iterator;


/**
 * A dataset is a simple transaction database store, which 
 * *may* perform some indexing for occurence delivery.
 * 
 * All actual implementations are package-visible - use DatasetFactory
 */
public abstract class Dataset {
	public final DatasetCounters counters;
	
	/**
	 * This constructor is only here to please the compiler
	 */
	protected Dataset(DatasetCounters counted) {
		this.counters = counted;
	}
	
	public abstract Iterator<TransactionReader> getSupport(int item);
	
	abstract Dataset project(int extension, DatasetCounters extensionCounters);
	
	/**
	 * Some lazy implementations may keep useless items in their transactions
	 * These will override this method so that such items will be ignored by DatasetCounters too 
	 */
	int[] getItemsIgnoredForCounting() {
		return new int[0];
	}
	
	public int ppTest(int extension) {
		return -1;
	}
}
