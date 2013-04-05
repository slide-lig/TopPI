package fr.liglab.lcm.mapred.groupers;

import gnu.trove.TIntCollection;

public class Slicer extends Grouper {

	public final int itemsPerGroup;
	
	public Slicer(int totalGroupCount, int greatestItem) {
		this.itemsPerGroup = (greatestItem / totalGroupCount) + 1;
	}
	
	public final int getGid(int item) {
		return item / this.itemsPerGroup;
	}

	public void fillWithGroupItems(TIntCollection collection, int gid, int greatestIID) {
		
		int roof = (gid+1) * this.itemsPerGroup;
		
		if (roof > greatestIID) {
			roof = greatestIID + 1;
		}
		
		for (int i=(gid * this.itemsPerGroup); i < roof; i++) {
			collection.add(i);
		}
	}
}
