package fr.liglab.lcm.internals;

import java.util.Iterator;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.map.TIntIntMap;

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
		this.rebaser = new Rebaser(this);
		super.prepareOccurences();
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
