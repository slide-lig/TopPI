package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Iterator;

/**
 * a ConcatenatedDataset rebased at first-loading time 
 * use it with a RebaserCollector
 */
public class RebasedConcatenatedDataset extends ConcatenatedDataset 
	implements RebasedDataset {
	
	private Rebaser rebaser; 
	
	public int[] getReverseMap() {
		return this.rebaser.getReverseMap();
	}
	
	/**
	 * Initial dataset constructor
	 * 
	 * the difference with parent class is in the overloaded sub-function "filter"
	 */
	public RebasedConcatenatedDataset(final int minimumsupport, final Iterator<int[]> transactions) {
		super(minimumsupport, transactions);
	}
	
	@Override
	protected TIntIntMap prepareOccurences() { 
		TIntIntIterator counts = this.supportCounts.iterator();
		TIntIntMap indexesMap = new TIntIntHashMap(this.supportCounts.size());
		
		this.rebaser = new Rebaser(this);
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		
		while (counts.hasNext()) {
			counts.advance();
			int rebasedItem = rebasing.get(counts.key());
			this.occurrences.put(rebasedItem, new int[counts.value()] );
			indexesMap.put(rebasedItem, 0);
		}
		
		return indexesMap;
	}
	
	@Override
	protected void filter(Iterable<int[]> transactions, TIntIntMap indexesMap) {
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		
		int i = 1;
		int tIndex = 0;
		
		for (int[] transaction : transactions) {
			int length = 0;
			
			for (int item : transaction) {
				if (rebasing.containsKey(item)) {
					int rebased = rebasing.get(item);
					this.concatenated[i] = rebased;
					
					int occurrenceIndex = indexesMap.get(rebased);
					this.occurrences.get(rebased)[occurrenceIndex] = tIndex;
					indexesMap.put(rebased, occurrenceIndex + 1);
					
					length++;
					i++;
				}
			}
			
			if (length > 0) {
				this.concatenated[tIndex] = length;
				tIndex = i;
				i++;
			}
		}
	}
}
