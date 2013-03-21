package fr.liglab.lcm.mapred;

import java.io.IOException;
import java.util.TreeSet;

import fr.liglab.lcm.util.ItemAndBigSupport;

public class ItemContigousReducer extends ItemGroupingReducer {
	
	@Override
	protected void computeGroupsFromFreqHeap(TreeSet<ItemAndBigSupport> heap, long frequentWordsCount, Context context) 
			throws IOException, InterruptedException {
		
		int itemsPerGroup = heap.size() / this.nbGroups;
		int gItemCount=0,gid=0,rebased=0;
		
		for (ItemAndBigSupport entry : heap) {
			keyW.set(entry.item);
			valueW.set(gid, rebased);
			context.write(keyW, valueW);
			rebased++;
			
			gItemCount ++;
			if (gItemCount == itemsPerGroup) {
				gid++;
				gItemCount = 0;
			}
		}
	}
}