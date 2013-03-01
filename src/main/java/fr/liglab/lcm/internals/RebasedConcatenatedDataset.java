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
	protected void prepareOccurences() { 
		TIntIntIterator counts = this.supportCounts.iterator();
		int i = 0;
		
		this.rebaser = new Rebaser(this);
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		
		while (counts.hasNext()) {
			counts.advance();
			int rebasedItem = rebasing.get(counts.key());
			
			this.occurrences[i] = counts.value();
			this.occurrencesIndexes.put(rebasedItem, i);
			i += counts.value() + 1;
		}
	}
	
	@Override
	protected void filter(Iterable<int[]> transactions) {
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		TIntIntMap indexesMap = new TIntIntHashMap(occurrencesIndexes);
		int i = 1;
		int tIndex = 0;
		
		for (int[] transaction : transactions) {
			int length = 0;
			
			for (int item : transaction) {
				if (rebasing.containsKey(item)) {
					int rebased = rebasing.get(item);
					this.concatenated[i] = rebased;
					
					int occurrenceIndex = indexesMap.get(rebased) + 1;
					this.occurrences[occurrenceIndex] = tIndex;
					indexesMap.put(rebased, occurrenceIndex);
					
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
