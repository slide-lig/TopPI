package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

public class InitialDataset extends Dataset {
	
	protected int transactionsCount = 0;
	protected int[] discoveredClosure;
	
	/**
	 * Filtered transactions : they contain only items whose support count is in [minusp, 100% [
	 */
	protected ArrayList<int[]> data = new ArrayList<int[]>();
	
	/**
	 * item => List<transactions containing item>
	 * In other words, occurrences is projections' data field, pre-computed
	 */
	protected TIntObjectHashMap<ArrayList<int[]>> occurrences = new TIntObjectHashMap<ArrayList<int[]>>();
	
	/**
	 * "transactions" iterator will be traversed only once. Though, references 
	 * to provided transactions will be kept and re-used during instanciation.
	 * None will be kept after.  
	 */
	public InitialDataset(int minimumsupport, Iterator<int[]> transactions) {
		minsup = minimumsupport;
		
		ArrayList<int[]> copy = new ArrayList<int[]>();
		TIntIntMap supportCounts = new TIntIntHashMap();
		
		// count
		while (transactions.hasNext()) {
			int[] transaction = transactions.next();
			copy.add(transaction);
			
			for (int i = 0; i < transaction.length; i++) {
				supportCounts.adjustOrPutValue(transaction[i], 1, 1);
			}
		}
		
		// predict frequent-only-transactions count - aka future closure's support count
		nextTransaction: for( Iterator<int[]> it = copy.iterator(); it.hasNext(); ) {
			int[] transaction = it.next();
			
			for (int item : transaction) {
				if (supportCounts.get(item) >= minsup) {
					continue nextTransaction;
				}
			}
			
			it.remove();
		}
		
		transactionsCount = copy.size();
		
		// filter unfrequent and closure
		ItemsetsFactory builder = new ItemsetsFactory();
		for (TIntIntIterator count = supportCounts.iterator(); count.hasNext();){
			count.advance();
			
			if (count.value() < minsup) {
				count.remove();
			} else if (count.value() == transactionsCount) {
				builder.add(count.key());
				count.remove();
			}
		}
		
		discoveredClosure = builder.get();
		
		for (int[] inputTransaction : copy) {
			for (int item : inputTransaction) {
				if (supportCounts.containsKey(item)) {
					builder.add(item);
				}
			}
			
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
			
			data.add(filtered);
		}
	}

	@Override
	public int getTransactionsCount() {
		return transactionsCount;
	}

	@Override
	public int[] getDiscoveredClosureItems() {
		return discoveredClosure;
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
	
	/**
	 * This iterator will enumerate candidate items such that
	 *  - candidate's support count is in [minsup, transactionsCount[
	 *  - candidate > core_item
	 *  - no item in ] core_item; candidate [ has the same support as candidate (aka fast-prefix-preservation test)
	 * Typically, core_item = extension item
	 */
	protected class FastPPCheckDecorator implements TIntIterator {
		
		private TIntIterator decorated;
		
		/**
		 * @param original an iterator on frequent items
		 * @param min 
		 */
		public FastPPCheckDecorator(TIntIterator original, int core_item) {
			decorated = original;
		}
		
		

		public boolean hasNext() {
			// TODO Auto-generated method stub
			return false;
		}
		
		/**
		 * NOT IMPLEMENTED
		 */
		public void remove() {
			throw new NotImplementedException();
		}

		public int next() {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
}
