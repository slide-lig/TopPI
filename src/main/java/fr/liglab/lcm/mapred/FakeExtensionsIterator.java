package fr.liglab.lcm.mapred;

import fr.liglab.lcm.internals.FrequentsIterator;
import gnu.trove.iterator.TIntIterator;

public final class FakeExtensionsIterator implements FrequentsIterator {
	
	private final TIntIterator wrapped;
	private Integer limit = null;
	
	public FakeExtensionsIterator(TIntIterator actualExtensions) {
		this.wrapped = actualExtensions;
	}
	
	@Override
	public int next() {
		if (this.wrapped.hasNext()) {
			if (this.limit == null) {
				return this.wrapped.next();
			} else {
				int next = this.wrapped.next();
				if (next > this.limit) {
					return -1;
				} else {
					return next;
				}
			}
		} else {
			return -1;
		}
	}


	public void interruptAt(int biggestItemOutputted) {
		this.limit = new Integer(biggestItemOutputted);
	}
}
