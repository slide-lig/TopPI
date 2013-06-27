package fr.liglab.mining.internals;

import fr.liglab.mining.internals.FrequentsIterator;

/**
 * Decorates a FrequentsIterator : all items from the wrapped iterator will be renamed according 
 * to the map provided at instantiation.
 * 
 * This decorator won't check that items fit in renaming array.
 */
public class FrequentsIteratorRenamer implements FrequentsIterator {
	
	private final int[] renaming;
	private final FrequentsIterator wrapped;
	
	public FrequentsIteratorRenamer(final FrequentsIterator decorated, final int[] itemsRenaming) {
		this.renaming = itemsRenaming;
		this.wrapped = decorated;
	}
	
	@Override
	public int next() {
		final int next = this.wrapped.next();
		if (next >= 0) {
			return this.renaming[next];
		} else {
			return -1;
		}
	}

	@Override
	public int peek() {
		final int next = this.wrapped.peek();
		if (next >= 0) {
			return this.renaming[next];
		} else {
			return -1;
		}
	}

	@Override
	public int last() {
		return this.renaming[this.wrapped.last()];
	}
}
