package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemAndSupport;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

import java.util.PriorityQueue;

/**
 * A class able to compute rebasing maps for a "target dataset"
 * 
 * Maps are computed at instanciation - do it once dataset's support 
 * counts are complete !
 */
public class Rebaser {
	
	protected int[] reverseMap = null;
	protected TIntIntMap rebaseMap = null;
	
	/**
	 * @return an array such that, for every i outputted by this dataset (as 
	 * closure, candidate, ...) , reverseMap[i] = i's original item index
	 */
	public int[] getReverseMap() {
		return this.reverseMap;
	}
	
	public TIntIntMap getRebasingMap() {
		return this.rebaseMap;
	}
	
	/**
	 * Prepare a rebaser for the given dataset
	 */
	public Rebaser(Dataset targetDataset) {
		final TIntIntMap targetCounts =  targetDataset.supportCounts;
		final int[] targetClosure = targetDataset.discoveredClosure;
		
		final TIntIntMap newCounts = new TIntIntHashMap(targetCounts.size());
		
		int mapSize = targetCounts.size() + targetClosure.length;

		this.rebaseMap = new TIntIntHashMap(mapSize);
		this.reverseMap = new int[mapSize];
		
		if (mapSize > 0) {
			final PriorityQueue<ItemAndSupport> heap = new 
					PriorityQueue<ItemAndSupport>(targetCounts.size());
			
			targetCounts.forEachEntry(new TIntIntProcedure() {
				public boolean execute(int key, int value) {
					heap.add(new ItemAndSupport(key, value));
					return true;
				}
			});
			
			for (int i = 0; i < targetClosure.length; i++) {
				this.reverseMap[i] = targetClosure[i];
				targetClosure[i] = i;
			}
			
			ItemAndSupport entry = heap.poll();
			for (int i=targetClosure.length; entry != null; i++) {
				this.reverseMap[i] = entry.item;
				this.rebaseMap.put(entry.item, i);
				newCounts.put(i, entry.support);
				entry = heap.poll();
			}
		}
		
		targetDataset.supportCounts = newCounts;
	}
}
