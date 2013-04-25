package fr.liglab.lcm.internals;

import java.util.Iterator;


/**
 * A dataset is a simple transaction database store, which 
 * *may* perform some indexing for occurence delivery.
 * 
 * All actual implementations are package-visible - use DatasetFactory
 * 
 * // TODO put ppTest somewhere else ?
 */
public abstract class Dataset {
	public abstract Iterator<TransactionReader> getSupport(int item);
	public abstract DatasetCounters getCounters();
	
	abstract Dataset project(int extension, DatasetCounters extensionCounters);
	
	int[] getItemsIgnoredForCounting() {
		return new int[0];
	}
	
	public int ppTest() {
		return -1;
	}
}
