package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class should be used for initial 
 */
public class InitialDataset extends Dataset {
	
	private ArrayList<int[]> data = new ArrayList<int[]>();
	
	/**
	 * "transactions" iterator will be traversed only once. Though, references 
	 * to provided transactions will be kept and re-used during instanciation.
	 * None will be kept after.  
	 */
	public InitialDataset(int minimumsupport, Iterator<int[]> transactions) {
		minsup = minimumsupport;
		
		ArrayList<int[]> copy = new ArrayList<int[]>();
		TIntIntMap supportCounts = new TIntIntHashMap();
		
		while (transactions.hasNext()) {
			int[] transaction = transactions.next();
			copy.add(transaction);
			
			for (int i = 0; i < transaction.length; i++) {
				supportCounts.adjustOrPutValue(transaction[i], 1, 1);
			}
		}
		
		for (TIntIntIterator count = supportCounts.iterator(); count.hasNext();){
			count.advance();
			
			if (count.value() < minsup) {
				count.remove();
			}
		}
		
		/**
		 * TODO : find closure
		 * /!\ boring corner case : transactions containing only infrequent items should be removed
		 * thus an item may have a support count == #finalTransactionCount < #inputTransactionCount
		 */
		
		for (int[] inputTransaction : copy) {
			Itemsets.startBuilder();
			Itemsets.addToBuilder(frequent_item);
			data.add(Itemsets.getBuilt());
			
			// and generate tid-list by the way
		}
		
		
	}

	@Override
	public int getTransactionsCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int[] getDiscoveredClosureItems() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Dataset getProjection(int extension) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TIntIterator getCandidatesIterator() {
		// TODO Auto-generated method stub
		return null;
	}

}
