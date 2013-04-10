package fr.liglab.lcm.util;

import fr.liglab.lcm.internals.ExtensionsIterator;
import gnu.trove.iterator.TIntIterator;

/**
 * FIXME : actually, ExtensionsIterator is useless !
 */
public class FakeExtensionsIterator implements ExtensionsIterator {

	private final int[] sortedFrequents;
	private final TIntIterator wrapped;

	public FakeExtensionsIterator(int[] sortedFrequents, TIntIterator wrapped) {
		this.sortedFrequents = sortedFrequents;
		this.wrapped = wrapped;
	}

	public int[] getSortedFrequents() {
		return this.sortedFrequents;
	}

	public synchronized int getExtension() {
		if (wrapped.hasNext()) {
			return wrapped.next();
		} else {
			return -1;
		}
	}

}
