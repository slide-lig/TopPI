package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

/**
 * This dataset internally performs
 *  - item ordering and filtering, ie. transactions contain, in ascending order, items whose support count is in [minusp, 100% [
 *  - occurence delivery
 *  - fast prefix-preserving test (see inner class CandidatesIterator)
 */
public class InitialDataset extends Dataset {
	
	protected int transactionsCount = 0;
	protected int[] discoveredClosure;
	
	/**
	 * Filtered'n'ordered transactions
	 */
	protected ArrayList<int[]> data = new ArrayList<int[]>();
	
	/**
	 * frequent item => List<transactions containing item>
	 * In other words, this map provides projections "for free"
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
		
		// create filtered transactions
		for (int[] inputTransaction : copy) {
			for (int item : inputTransaction) {
				if (supportCounts.containsKey(item)) {
					builder.add(item);
				}
			}
			
			int [] filtered = builder.get();
			Arrays.sort(filtered); // TODO perform insertion sort directly in ItemsetsFactory
			
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
		return new CandidatesIterator(Integer.MIN_VALUE);
	}
	
	/**
	 * Iterates on candidates items such that
	 * - their support count is in [minsup, transactionsCount[ ,
	 *  - candidate > core_item
	 *  - no item in ] core_item; candidate [ has the same support as candidate (aka fast-prefix-preservation test)
	 * Typically, core_item = extension item
	 */
	protected class CandidatesIterator implements TIntIterator {
		private int next_index;
		private int[] candidates;
		
		/**
		 * @param original an iterator on frequent items
		 * @param min 
		 */
		public CandidatesIterator(int core_item) {
			next_index = -1;
			
			int[] frequentItems = occurrences.keys();
			Arrays.sort(frequentItems);
			
			if (core_item == Integer.MIN_VALUE) { // FIXME
				candidates = frequentItems;
			} else {
				int core_index = 0;
				while (core_index < frequentItems.length && frequentItems[core_index] <= core_item) core_index++;
				
				if (core_index < frequentItems.length) {
					candidates = new int[frequentItems.length - core_index];
					System.arraycopy(frequentItems, core_index, candidates, 0, candidates.length);
				}
			}
			
			findNext();
		}
		
		private void findNext() {
			next_index++;
			
			while (0 <= next_index && next_index < candidates.length) {
				if (prefixPreservingTest(candidates[next_index])) {
					break;
				} else {
					next_index++;
				}
			}
		}
		
		/**
		 * @return true if there is no int j in ] core_item; candidate [ having the same support as candidate 
		 */
		private boolean prefixPreservingTest(int candidate) {
			ArrayList<int[]> candidateOccurrences = occurrences.get(candidate);
			
			for (int i=0; candidates[i] < candidate; i++) {
				int j = candidates[i];
				ArrayList<int[]> jOccurrences = occurrences.get(j);
				
				//  otherwise we have  supp(j) < supp(candidate) : no need to worry about j
				if (jOccurrences.size() >= candidateOccurrences.size()) {
					boolean jBelongsToAllCandidateOccurrences = true;
					for (int[] transaction : candidateOccurrences) {
						if (Arrays.binarySearch(transaction, j) < 0) {
							jBelongsToAllCandidateOccurrences = false;
							break;
						}
					}
					
					if (jBelongsToAllCandidateOccurrences) {
						return false;
					}
				}
			}
			
			return true;
		}
		
		public boolean hasNext() {
			return next_index >= 0;
		}
		
		public int next() {
			int old_i = next_index;
			findNext();
			return candidates[old_i];
		}

		/**
		 * You probably thought this method was alive. NOPE. It's just Chuck Testa !
		 */
		public void remove() {
			throw new NotImplementedException();
		}
	}
}
