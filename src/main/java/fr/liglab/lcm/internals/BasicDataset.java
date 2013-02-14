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
 *  - basic reduction : transactions contain only items having a support count in [minusp, 100% [
 *  - item ordering in transactions (in ascending order)
 *  - occurence delivery (actually, filtered transactions are only referenced in occurences lists)
 *  - fast prefix-preserving test (see inner class CandidatesIterator)
 */
public class BasicDataset extends Dataset {
	
	protected int coreItem = Integer.MIN_VALUE; // default value for the initial itemset
	protected int transactionsCount = 0;
	protected int[] discoveredClosure;
	
	/**
	 * frequent item => List<transactions containing item>
	 * In other words, this map provides projections "for free"
	 * 
	 * Note that transactions are added in the same order in all occurences-ArrayLists. This property is used in CandidatesIterator's prefix-preserving test
	 */
	protected TIntObjectHashMap<ArrayList<int[]>> occurrences = new TIntObjectHashMap<ArrayList<int[]>>();
	
	/**
	 * "transactions" iterator will be traversed only once. Though, references 
	 * to provided transactions will be kept and re-used during instanciation.
	 * None will be kept after.  
	 */
	public BasicDataset(final int minimumsupport, final Iterator<int[]> transactions) {
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
		}
	}
	
	
	/**
	 * Projection constructor
	 * projectedSupport is supposed to be projectedItem support
	 * items in coreSupport's transactions are assumed to be already sorted
	 * projectedItem will never re-appear or be outputted by this object
	 */
	protected BasicDataset(final int minimumsupport, final ArrayList<int[]> projectedSupport, final int projectedItem) {
		minsup = minimumsupport;
		coreItem = projectedItem;
		transactionsCount = projectedSupport.size();
		
		//////// COUNT
		TIntIntMap supportCounts = new TIntIntHashMap();
		
		for (int[] transaction : projectedSupport) {
			for (int i = 0; i < transaction.length; i++) {
				// coz we assume coreItem'support will 100% + we don't want it in the computed closure
				if (transaction[i] != coreItem) {
					supportCounts.adjustOrPutValue(transaction[i], 1, 1);
				}
			}
		}
		
		/////// FILTER COUNT
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
		
		/////// REDUCE DATA
		for (int[] inputTransaction : projectedSupport) {
			for (int item : inputTransaction) {
				if (supportCounts.containsKey(item)) {
					builder.add(item);
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
		
		/////// PROFIT
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
		return new BasicDataset(minsup, occurrences.get(extension), extension);
	}

	
	
	
	
	
	@Override
	public TIntIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}
	
	/**
	 * Iterates on candidates items such that
	 * - their support count is in [minsup, transactionsCount[ ,
	 *  - candidate > coreItem
	 *  - no item < candidate has the same support as candidate (aka fast-prefix-preservation test)
	 *    => assuming items from previously found patterns have been removed !!
	 * coreItem = extension item (if it exists)
	 */
	protected class CandidatesIterator implements TIntIterator {
		private int next_index;
		private final int[] candidates;
		private final int[] frequentItems;
		
		/**
		 * @param original an iterator on frequent items
		 * @param min 
		 */
		public CandidatesIterator() {
			next_index = -1;
			
			frequentItems = occurrences.keys();
			Arrays.sort(frequentItems);
			
			int coreItemIndex = Arrays.binarySearch(frequentItems, coreItem);
			if (coreItemIndex >= 0) {
				throw new RuntimeException("Unexpected : coreItem appears in frequentItems !");
			}
			
			// binarySearch returns -(insertion_point)-1
			// where insertion_point == index of first element greater OR a.length
			int candidatesStartIndex = -coreItemIndex - 1;
			
			if (candidatesStartIndex == 0) {
				candidates = frequentItems;
				findNext();
			} else if (candidatesStartIndex < frequentItems.length) {
				candidates = new int[frequentItems.length - candidatesStartIndex];
				System.arraycopy(frequentItems, candidatesStartIndex, candidates, 0, candidates.length);
				findNext();
				
			} else { // candidatesStartIndex == frequentItems.length , ie. STOP
				candidates = null;
			}
		}
		
		private void findNext() {
			next_index++;
			
			while (0 <= next_index) {
				if (next_index == candidates.length) {
					next_index = -1;
					break;
				} else if (prefixPreservingTest(candidates[next_index])) {
					break;
				} else {
					next_index++;
				}
			}
		}
		
		/**
		 * @return true if there is no int j in [0; candidate [ having the same support as candidate 
		 */
		private boolean prefixPreservingTest(final int candidate) {
			ArrayList<int[]> candidateOccurrences = occurrences.get(candidate);
			
			for (int i=0; frequentItems[i] < candidate; i++) {
				int j = frequentItems[i];
				ArrayList<int[]> jOccurrences = occurrences.get(j);
				
				//  otherwise we have  supp(j) < supp(candidate) : no need to worry about j
				if (jOccurrences.size() >= candidateOccurrences.size()) {
					if (isAincludedInB(candidateOccurrences, jOccurrences)) {
						return false;
					}
				}
			}
			
			return true;
		}
		
		/**
		 * @return true if A is included in B, assuming they share array pointers (appended in the same order)
		 */
		private boolean isAincludedInB(final ArrayList<int[]> a, final ArrayList<int[]> b) {
			Iterator<int[]> aIt = a.iterator();
			Iterator<int[]> bIt = b.iterator();
			
			int[] transactionA = null;
			int[] transactionB = null;
			
			// FIXME maybe some readahead could avoid complete traversal 
			while (aIt.hasNext()) {
				transactionA = aIt.next();
				
				while (true) {
					transactionB = bIt.next();
					if (transactionA == transactionB) {
						break;
					}
					if (!bIt.hasNext()) { // couldn't find transactionA in B
						return false;
					}
				}
			}
			
			return transactionA == transactionB;
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
