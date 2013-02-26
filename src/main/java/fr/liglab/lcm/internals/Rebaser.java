package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemAndSupport;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

import java.util.PriorityQueue;

/**
 * A class able to compute rebasing maps for a "target dataset"
 * 
 * Maps are computed only on the first invocation of 
 * getReverseMap or getRebasingMap - call them once dataset's support 
 * counts are complete !
 */
public class Rebaser {
	
	protected int[] reverseMap = null;
	protected TIntIntMap rebaseMap = null;
	
	protected final TIntIntMap targetCounts;
	protected final int[] targetClosure;
	
	/**
	 * @return an array such that, for every i outputted by this dataset (as 
	 * closure, candidate, ...) , reverseMap[i] = i's original item index
	 */
	public int[] getReverseMap() {
		if (this.reverseMap == null) {
			buildMaps();
		}
		return this.reverseMap;
	}
	
	public TIntIntMap getRebasingMap() {
		if (this.rebaseMap == null) {
			buildMaps();
		}
		return this.rebaseMap;
	}
	
	/**
	 * Prepare a rebaser for the given dataset
	 */
	public Rebaser(Dataset targetDataset) {
		this.targetCounts =  targetDataset.supportCounts;
		this.targetClosure = targetDataset.discoveredClosure;
	}
	
	protected void buildMaps() {
		int mapSize = this.targetCounts.size() + this.targetClosure.length;

		this.rebaseMap = new TIntIntHashMap(mapSize);
		this.reverseMap = new int[mapSize];
		
		if (mapSize > 0) {
			final PriorityQueue<ItemAndSupport> heap = new 
					PriorityQueue<ItemAndSupport>(this.targetCounts.size());
			
			this.targetCounts.forEachEntry(new TIntIntProcedure() {
				public boolean execute(int key, int value) {
					heap.add(new ItemAndSupport(key, value));
					return true;
				}
			});
			
			for (int i = 0; i < this.targetClosure.length; i++) {
				this.reverseMap[i] = this.targetClosure[i];
				this.targetClosure[i] = i;
			}
			
			ItemAndSupport entry = heap.poll();
			for (int i=this.targetClosure.length; entry != null; i++) {
				this.reverseMap[i] = entry.item;
				this.rebaseMap.put(entry.item, i);
				entry = heap.poll();
			}
		}
	}
}
