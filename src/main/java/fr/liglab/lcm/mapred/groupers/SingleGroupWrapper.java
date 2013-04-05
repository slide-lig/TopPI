package fr.liglab.lcm.mapred.groupers;

import gnu.trove.TIntCollection;

/**
 * Filter another Grouper's output such that when GID != singleGroupId
 * it returns -1 (which will be ignored later during group generation)
 */
public class SingleGroupWrapper extends Grouper {
	
	private final Grouper decorated;
	private final int singleGroupId;
	
	public SingleGroupWrapper(Grouper wrapped, int onlyGroupAllowed) {
		this.decorated = wrapped;
		this.singleGroupId = onlyGroupAllowed;
	}

	public int getGid(int item) {
		if (this.decorated.getGid(item) != this.singleGroupId) {
			return -1;
		} else {
			return this.singleGroupId;
		}
	}

	public void fillWithGroupItems(TIntCollection collection, int gid, int greatestIID) {
		this.decorated.fillWithGroupItems(collection, gid, greatestIID);
	}
}
