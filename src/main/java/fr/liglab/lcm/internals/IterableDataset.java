package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;

public abstract class IterableDataset extends Dataset {

	IterableDataset(int minimum_support, int core_item) {
		super(minimum_support, core_item);
	}

	protected abstract TIntIterator readTransaction(int tid);

	protected abstract TIntList getTidList(int item);

}
