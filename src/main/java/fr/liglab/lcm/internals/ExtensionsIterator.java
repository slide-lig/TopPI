package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;

public interface ExtensionsIterator extends TIntIterator {
	public int[] getSortedFrequents();
}
