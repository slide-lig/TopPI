package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.CopyIteratorDecorator;
import fr.liglab.lcm.util.ItemsetsFactory;
import fr.liglab.lcm.util.SortedItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

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
	
	protected final int coreItem;
	protected final int transactionsCount;
	
	/**
	 * frequent item => List<transactions containing item>
	 * In other words, this map provides projections "for free"
	 * 
	 * Note that transactions are added in the same order in all occurences-ArrayLists. This property is used in CandidatesIterator's prefix-preserving test
	 */
	protected final TIntObjectHashMap<ArrayList<int[]>> occurrences = new TIntObjectHashMap<ArrayList<int[]>>();
	
	/**
	 * Initial dataset constructor
	 * 
	 * "transactions" iterator will be traversed only once. Though, references 
	 * to provided transactions will be kept and re-used during instanciation.
	 * None will be kept after.  
	 */
	public BasicDataset(final int minimumsupport, final Iterator<int[]> transactions) {
		minsup = minimumsupport;
		
		// in initial dataset, all items are candidate => all items < coreItem
		coreItem = Integer.MAX_VALUE;
		
		CopyIteratorDecorator<int[]> transactionsCopier = new CopyIteratorDecorator<int[]>(transactions); 
		genSupportCounts(transactionsCopier);
		transactionsCount = transactionsCopier.size();
		
		genClosureAndFilterCount();
		
		reduceAndBuildOccurrences(transactionsCopier, new SortedItemsetsFactory());
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
		
		genSupportCounts(projectedSupport.iterator());
		// coreItem is known to have a 100% support,  we don't want it in the computed closure
		supportCounts.remove(coreItem);
		
		genClosureAndFilterCount();
		
		reduceAndBuildOccurrences(projectedSupport, new ItemsetsFactory());
	}
	
	/**
	 * Reduce the given dataset and build occurrences list by the way
	 * 
	 * All transactions will be filtered (keeping only items existing in supportCounts) and 
	 * constructed via the provided builder
	 */
	protected void reduceAndBuildOccurrences(Iterable<int[]> dataset, ItemsetsFactory builder) {
		builder.get(); // reset builder, just to be sure
		TIntSet retained = this.supportCounts.keySet();
		
		for (int[] inputTransaction : dataset) {
			for (int item : inputTransaction) {
				if (retained.contains(item)) {
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
	}
	
	
	
	

	@Override
	public int getTransactionsCount() {
		return transactionsCount;
	}

	@Override
	public Dataset getProjection(int extension) {
		return new BasicDataset(minsup, occurrences.get(extension), extension);
	}

	
	
	
	
	
	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}
	
	/**
	 * Iterates on candidates items such that
	 * - their support count is in [minsup, transactionsCount[ ,
	 *  - candidate < coreItem
	 *  - no item > candidate has the same support as candidate (aka fast-prefix-preservation test)
	 *    => assuming items from previously found patterns (including coreItem) have been removed !!
	 * coreItem = extension item (if it exists)
	 */
	protected class CandidatesIterator implements TIntIterator, ExtensionsIterator {
		private int next_index;
		private final int candidatesLength; // candidates is frequentItems[0:candidatesLength[
		private final int[] frequentItems;
		
		public int[] getSortedFrequents() {
			return frequentItems;
		}
		
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
			candidatesLength = -coreItemIndex - 1; 
			
			if (candidatesLength >= 0) {
				findNext();
			}
		}
		
		private void findNext() {
			next_index++;
			
			while (0 <= next_index) {
				if (next_index == candidatesLength) {
					next_index = -1;
					break;
				} else if (prefixPreservingTest(next_index)) {
					break;
				} else {
					next_index++;
				}
			}
		}
		
		/**
		 * @return true if there is no int j > candidate having the same support as candidate 
		 */
		private boolean prefixPreservingTest(final int candidateIndex) {
			final int candidate = frequentItems[candidateIndex];
			ArrayList<int[]> candidateOccurrences = occurrences.get(candidate);
			
			for (int i=candidateIndex + 1; i < frequentItems.length; i++) {
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
			return frequentItems[old_i];
		}

		/**
		 * You probably thought this method was alive. NOPE. It's just Chuck Testa !
		 */
		public void remove() {
			throw new NotImplementedException();
		}
	}
}
