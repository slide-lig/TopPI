package fr.liglab.lcm.internals;

import gnu.trove.map.TIntIntMap;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A dataset where items are re-indexed by frequency (only at first loading) 
 * such that 0 will be the most frequent item. Then it's projected as BasicDataset
 * 
 * WARNING : once instanciated, all items outputted by this object and its 
 * projections should be considered as rebased
 * 
 * use getReverseMap()
 */
public class RebasedBasicDataset extends BasicDataset implements RebasedDataset {
	
	protected Rebaser rebaser; 
	
	public RebasedBasicDataset(int minimumsupport, Iterator<int[]> transactions) {
		super(minimumsupport, transactions);
	}
	
	public int[] getReverseMap() {
		return this.rebaser.getReverseMap();
	}
	
	@Override
	/**
	 * Only difference with its parent implementation should be the "newBase" apparition
	 */
	protected void reduceAndBuildOccurrences(Iterable<int[]> dataset, ItemsetsFactory builder) {
		builder.get(); // reset builder, just to be sure

		this.rebaser = new Rebaser(this);
		TIntIntMap newBase = this.rebaser.getRebasingMap();
		
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
