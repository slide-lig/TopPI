package fr.liglab.lcm.mapred;

import java.io.IOException;
import java.util.TreeSet;

import fr.liglab.lcm.util.ItemAndBigSupport;

public class ItemContigousReducer extends ItemGroupingReducer {
	
	@Override
	protected void computeGroupsFromFreqHeap(TreeSet<ItemAndBigSupport> heap, long frequentWordsCount, Context context) 
			throws IOException, InterruptedException {
		
		long idealSupportSum = frequentWordsCount / this.nbGroups;
		long currentSupportSum = 0;
		int gid=0,rebased=0;
		
		for (ItemAndBigSupport entry : heap) {
			keyW.set(entry.item);
			valueW.set(gid, rebased);
			context.write(keyW, valueW);
			rebased++;
			
			currentSupportSum += entry.support;
			if (currentSupportSum >= idealSupportSum) {
				gid++;
				currentSupportSum = 0;
			}
		}
	}
}
