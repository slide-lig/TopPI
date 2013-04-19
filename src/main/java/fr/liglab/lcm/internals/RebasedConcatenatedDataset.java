package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;

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
	 * @throws DontExploreThisBranchException 
	 */
	public RebasedConcatenatedDataset(final int minimumsupport, final Iterator<int[]> transactions)
			throws DontExploreThisBranchException {
		
		super(minimumsupport, transactions);
	}
	
	@Override
	protected void prepareOccurences() {
		
		// Rebaser instanciation will nullify supportCounts - grab it while it's there ! 
		TIntIntIterator counts = this.supportCounts.iterator();
		
		this.rebaser = new Rebaser(this);
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		
		while (counts.hasNext()) {
			counts.advance();
			int rebasedItem = rebasing.get(counts.key());
			this.occurrences.put(rebasedItem, new TIntArrayList(counts.value()));
		}
	}
	
	@Override
	protected void filter(Iterable<int[]> transactions) {
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		
		int i = 1;
		int tIndex = 0;
		
		for (int[] transaction : transactions) {
			int length = 0;
			
			for (int item : transaction) {
				if (rebasing.containsKey(item)) {
					int rebased = rebasing.get(item);
					this.concatenated[i] = rebased;
					this.occurrences.get(rebased).add(tIndex);
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
