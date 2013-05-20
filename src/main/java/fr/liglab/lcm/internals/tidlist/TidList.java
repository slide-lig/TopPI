package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIterator;

public interface TidList {
	
	// FIXME - is it useless ?
	public TIntIterator get(final int item);

	public TIntIterable getIterable(final int item);

	public void addTransaction(final int item, final int transaction);

	public interface TIntIterable {
		public TIntIterator iterator();
	}
}
