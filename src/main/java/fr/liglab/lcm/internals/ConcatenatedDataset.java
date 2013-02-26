package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.CopyIteratorDecorator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

/**
 * Here all transactions are prefixed by their length and concatenated in a 
 * single int[] 
 * 
 * This dataset internally performs
 *  - basic reduction : transactions contain only items having a support count
 *    in [minusp, 100% [
 *  - occurrence delivery : occurrences are stored as indexes in the 
 *    concatenated array
 *  - fast prefix-preserving test (see inner class CandidatesIterator)
 */
public class ConcatenatedDataset extends Dataset {
	
	protected final int[] concatenated;
	protected final int coreItem;
	protected final int transactionsCount;
	
	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurences-arrays. This property is used in CandidatesIterator's prefix-preserving test
	 */
	protected final TIntObjectHashMap<TIntArrayList> occurrences = new TIntObjectHashMap<TIntArrayList>();
	
	/**
	 * Initial dataset constructor
	 * 
	 * "transactions" iterator will be traversed only once. Though, references 
	 * to provided transactions will be kept and re-used during instanciation.
	 * None will be kept after.  
	 */
	public ConcatenatedDataset(final int minimumsupport, final Iterator<int[]> transactions) {
		// in initial dataset, all items are candidate => all items < coreItem
		this.coreItem = Integer.MAX_VALUE;
		this.minsup = minimumsupport;
		
		CopyIteratorDecorator<int[]> transactionsCopier = new CopyIteratorDecorator<int[]>(transactions); 
		this.genSupportCounts(transactionsCopier);
		this.transactionsCount = transactionsCopier.size();
		
		int remainingItemsCount = genClosureAndFilterCount();
		this.prepareOccurences();
		this.concatenated = new int[remainingItemsCount + this.transactionsCount];
		
		this.filter(transactionsCopier);
	}
	
	protected void filter(final Iterable<int[]> transactions) {
		TIntSet retained = this.supportCounts.keySet();
		int i = 1;
		int tIndex = 0;
		
		for (int[] transaction : transactions) {
			int length = 0;
			
			for (int item : transaction) {
				if (retained.contains(item)) {
					this.concatenated[i] = item;
					this.occurrences.get(item).add(tIndex);
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
	
	protected ConcatenatedDataset(ConcatenatedDataset parent, int extension) {
		this.supportCounts = new TIntIntHashMap();
		this.minsup = parent.minsup;
		this.coreItem = extension;
		
		TIntArrayList occurrences = parent.occurrences.get(extension);
		this.transactionsCount = occurrences.size();
		
		TIntIterator iterator =  occurrences.iterator();
		while(iterator.hasNext()) {
			int tid = iterator.next();
			int length = parent.concatenated[tid];
			for (int i = tid + 1; i <= tid+length; i++) {
				this.supportCounts.adjustOrPutValue(parent.concatenated[i], 1, 1);
			}
		}
		
		supportCounts.remove(extension);
		int remainingItemsCount = genClosureAndFilterCount();
		this.prepareOccurences();
		
		TIntSet retained = this.supportCounts.keySet();
		this.concatenated = new int[remainingItemsCount + this.transactionsCount];
		int i = 1;
		int tIndex = 0;
		
		iterator = occurrences.iterator();
		while(iterator.hasNext()) {
			int parentTid = iterator.next();
			int parentLength = parent.concatenated[parentTid];
			int length = 0;
			
			for (int j = parentTid + 1; j <= parentTid + parentLength; j++) {
				int item = parent.concatenated[j];
				
				if (retained.contains(item)) {
					this.concatenated[i] = item;
					this.occurrences.get(item).add(tIndex);
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
	
	/**
	 * Pre-instanciate occurrences ArrayLists according to this.supportCounts
	 */
	protected void prepareOccurences() {
		TIntIntIterator counts = this.supportCounts.iterator();
		while (counts.hasNext()) {
			counts.advance();
			this.occurrences.put(counts.key(), new TIntArrayList(counts.value()));
		}
	}
	
	@Override
	public Dataset getProjection(int extension) {
		return new ConcatenatedDataset(this, extension);
	}

	@Override
	public int getTransactionsCount() {
		return transactionsCount;
	}

	@Override
	public TIntIterator getCandidatesIterator() {
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
	protected class CandidatesIterator implements TIntIterator {
		private int next_index;
		private final int candidatesLength; // candidates is frequentItems[0:candidatesLength[
		private final int[] frequentItems;
		
		/**
		 * @param original an iterator on frequent items
		 * @param min 
		 */
		public CandidatesIterator() {
			this.next_index = -1;
			
			this.frequentItems = occurrences.keys();
			Arrays.sort(this.frequentItems);
			
			int coreItemIndex = Arrays.binarySearch(this.frequentItems, coreItem);
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
			final int candidateSupport = supportCounts.get(candidate);
			TIntArrayList candidateOccurrences = occurrences.get(candidate);
			
			for (int i=candidateIndex + 1; i < frequentItems.length; i++) {
				int j = frequentItems[i];
				
				if (supportCounts.get(j) >= candidateSupport) {
					TIntArrayList jOccurrences = occurrences.get(j);
					if (isAincludedInB(candidateOccurrences, jOccurrences)) {
						return false;
					}
				}
			}
			
			return true;
		}
		
		/**
		 * Assumptions :
		 *  - both contain array indexes appended in increasing order
		 *  - you already tested that B.size >= A.size
		 * @return true if A is included in B 
		 */
		private boolean isAincludedInB(final TIntArrayList a, final TIntArrayList b) {
			TIntIterator aIt = a.iterator();
			TIntIterator bIt = b.iterator();
			
			int tidA = 0;
			int tidB = 0;
			
			while (aIt.hasNext() && bIt.hasNext()) {
				tidA = aIt.next();
				tidB = bIt.next();
				
				while (tidB < tidA && bIt.hasNext()) {
					tidB = bIt.next();
				}
				
				if (tidB > tidA) {
					return false;
				}
			}
			
			return tidA == tidB;
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
