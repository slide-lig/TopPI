package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemAndSupport;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.procedure.TIntIntProcedure;

import java.util.Iterator;
import java.util.PriorityQueue;

public class DatasetRebaserCounters extends DatasetCounters {
	
	/**
	 * an array such that, for every i outputted by this dataset (as 
	 * closure, candidate, ...) , reverseMap[i] = i's original item index
	 */
	public final int[] reverseMap;
	
	public TIntIntMap rebaseMap;

	public DatasetRebaserCounters(int minsup, Iterator<TransactionReader> transactions) {
		this(minsup, transactions, null);
	}
	
	public DatasetRebaserCounters(int minimumSupport, Iterator<TransactionReader> transactions, int[] ignoredItems) {
		super(minimumSupport, transactions, ignoredItems);
		
		final int nbItems = this.supportCounts.size();
		final int closureLength = this.closure.length;
		final int mapSize = nbItems + closureLength;
		
		this.rebaseMap = new TIntIntHashMap(mapSize);
		this.reverseMap = new int[mapSize];
		
		if (mapSize > 0) {
			final PriorityQueue<ItemAndSupport> heap = new PriorityQueue<ItemAndSupport>(nbItems);
			
			this.supportCounts.forEachEntry(new TIntIntProcedure() {
				public boolean execute(int key, int value) {
					heap.add(new ItemAndSupport(key, value));
					return true;
				}
			});
			
			for (int i = 0; i < closureLength; i++) {
				this.reverseMap[i] = this.closure[i];
				this.sortedFrequents[i] = i;
				this.closure[i] = i;
			}
			
			this.supportCounts.clear();
			
			ItemAndSupport entry = heap.poll();
			for (int i=closureLength; entry != null; i++) {
				this.reverseMap[i] = entry.item;
				this.rebaseMap.put(entry.item, i);
				
				this.supportCounts.put(i, entry.support);
				this.sortedFrequents[i] = i;
				
				entry = heap.poll();
			}
		}
	}
}
