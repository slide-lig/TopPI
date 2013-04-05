package fr.liglab.lcm.mapred.groupers;

import gnu.trove.TIntCollection;

public class RoundRobin extends Grouper {

	public final int nbGroups;
	
	public RoundRobin(final int total_group_count) {
		this.nbGroups = total_group_count;
	}
	
	public final int getGid(int item) {
		return item % this.nbGroups;
	}

	public void fillWithGroupItems(TIntCollection collection, int gid, int greatestIID) {
		for (int i=gid; i <= greatestIID; i += this.nbGroups) {
			collection.add(i);
		}
	}
}
