package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIterator;

public abstract class TidList {
	private boolean sorted;

	public TidList(boolean sorted) {
		this.sorted = sorted;
	}

	public final boolean isSorted() {
		return sorted;
	}

	public final void setSorted(boolean sorted) {
		this.sorted = sorted;
	}

	abstract public TIntIterator getTidList(final int item);

	abstract public void addTransaction(final int item, final int transaction);
}
