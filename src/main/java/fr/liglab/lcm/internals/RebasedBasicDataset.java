package fr.liglab.lcm.internals;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * A dataset where items are re-indexed by frequency (only at first loading) 
 * such that 0 will be the most frequent item. Then it's projected as BasicDataset
 * 
 * WARNING : once instanciated, all items outputted by this object and its 
 * projections should be considered as rebased
 * 
 * use getReverseMap()
 */
public class RebasedBasicDataset extends BasicDataset {
	
	protected int[] reverseMap;
	
	/**
	 * @return an array such that, for every i outputted by this dataset (as 
	 * closure, candidate, ...) , reverseMap[i] = i's original item index
	 */
	public int[] getReverseMap() {
		return reverseMap;
	}
	
	public RebasedBasicDataset(int minimumsupport, Iterator<int[]> transactions) {
		super(minimumsupport, transactions);
	}
	
	@Override
	/**
	 * Only difference with its parent implementation should be the "newBase" apparition
	 */
	protected void reduceAndBuildOccurrences(Iterable<int[]> dataset, TIntIntMap supportCounts, ItemsetsFactory builder) {
		builder.get(); // reset builder, just to be sure
		
		if (supportCounts.isEmpty()) {
			reverseMap = new int[0];
		} else {
			
			TIntIntMap newBase = buildRebasingMaps(supportCounts);
			
			for (int[] inputTransaction : dataset) {
				for (int item : inputTransaction) {
					if (newBase.containsKey(item)) {
						builder.add(newBase.get(item));
					}
				}
				
				if (!builder.isEmpty()) {
					int [] filtered = builder.get();
					
					for (int item : filtered) {
						ArrayList<int[]> tids = occurrences.get(item);
						if (tids == null) {
							tids = new ArrayList<int[]>();
							tids.add(filtered);
							occurrences.put(item, tids);
						} else {
							tids.add(filtered);
						}
					}
				}
			}
		}
	}

	
	protected TIntIntMap buildRebasingMaps(TIntIntMap supportCounts) {
		int mapSize = supportCounts.size();
		
		if (discoveredClosure.length > 0) {
			mapSize += discoveredClosure.length;
		}
		
		TIntIntMap rebaseMap = new TIntIntHashMap(mapSize);
		reverseMap = new int[mapSize];
		final PriorityQueue<ItemAndSupport> heap = new PriorityQueue<ItemAndSupport>(supportCounts.size());
		
		supportCounts.forEachEntry(new TIntIntProcedure() {
			public boolean execute(int key, int value) {
				heap.add(new ItemAndSupport(key, value));
				return true;
			}
		});
		
		for (int i = 0; i < discoveredClosure.length; i++) {
			reverseMap[i] = discoveredClosure[i];
			discoveredClosure[i] = i;
		}
		
		ItemAndSupport entry = heap.poll();
		for (int i=discoveredClosure.length; entry != null; i++) {
			reverseMap[i] = entry.item;
			rebaseMap.put(entry.item, i);
			entry = heap.poll();
		}
		
		return rebaseMap;
	}

}
