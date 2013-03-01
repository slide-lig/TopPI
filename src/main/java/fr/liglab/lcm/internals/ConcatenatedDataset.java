package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.CopyIteratorDecorator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
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
	 * Occurrences lists, prefixed by their length and concatenated
	 * TIDs (ie. transaction index in "concatenated") are added in the 
	 * same order in all occurences-arrays. 
	 * This property is used in CandidatesIterator's prefix-preserving test
	 */
	protected final int[] occurrences;
	
	/**
	 * item => occurrences list index in "occurrences"
	 */
	protected final TIntIntMap occurrencesIndexes;
	
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
		occurrences = new int[remainingItemsCount + supportCounts.size()];
		occurrencesIndexes = new TIntIntHashMap(supportCounts.size());
		this.prepareOccurences();
		
		this.concatenated = new int[remainingItemsCount + this.transactionsCount];
		this.filter(transactionsCopier);
	}
	
	protected void filter(final Iterable<int[]> transactions) {
		TIntIntMap indexesMap = new TIntIntHashMap(occurrencesIndexes);
		TIntSet retained = this.supportCounts.keySet();
		int i = 1;
		int tIndex = 0;
		
		for (int[] transaction : transactions) {
			int length = 0;
			
			for (int item : transaction) {
				if (retained.contains(item)) {
					this.concatenated[i] = item;
					
					int occurrenceIndex = indexesMap.get(item) + 1;
					this.occurrences[occurrenceIndex] = tIndex;
					indexesMap.put(item, occurrenceIndex);
					
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
		
		int occurrencesIndex = parent.occurrencesIndexes.get(extension);
		this.transactionsCount = parent.occurrences[occurrencesIndex];
		
		occurrencesIndex++;
		int maxIndex = occurrencesIndex + this.transactionsCount;
		
		for(; occurrencesIndex < maxIndex; occurrencesIndex++) {
			int tid = parent.occurrences[occurrencesIndex];
			int length = parent.concatenated[tid];
			for (int i = tid + 1; i <= tid+length; i++) {
				this.supportCounts.adjustOrPutValue(parent.concatenated[i], 1, 1);
			}
		}
		
		supportCounts.remove(extension);
		
		int remainingItemsCount = genClosureAndFilterCount();
		occurrences = new int[remainingItemsCount + supportCounts.size()];
		occurrencesIndexes = new TIntIntHashMap(supportCounts.size());
		this.prepareOccurences();
		
		TIntSet keeped = this.supportCounts.keySet();
		this.concatenated = new int[remainingItemsCount + this.transactionsCount];
		
		filterParent(parent, extension, keeped);
	}
	
	/**
	 * @param parent dataset
	 * @param projection item
	 * @param keeped items that will remain in our transactions
	 */
	protected void filterParent(ConcatenatedDataset parent, int extension, TIntSet keeped) {
		TIntIntMap indexesMap = new TIntIntHashMap(occurrencesIndexes);
		int i = 1;
		int tIndex = 0;
		
		int parentOccsIdx = parent.occurrencesIndexes.get(extension);
		int maxIndex = parentOccsIdx + parent.occurrences[parentOccsIdx] + 1;
		parentOccsIdx++;
		
		for(; parentOccsIdx < maxIndex; parentOccsIdx++) {
			int parentTid = parent.occurrences[parentOccsIdx];
			int parentLength = parent.concatenated[parentTid];
			int length = 0;
			
			for (int j = parentTid + 1; j <= parentTid + parentLength; j++) {
				int item = parent.concatenated[j];
				
				if (keeped.contains(item)) {
					this.concatenated[i] = item;
					
					int occurrenceIndex = indexesMap.get(item) + 1;
					this.occurrences[occurrenceIndex] = tIndex;
					indexesMap.put(item, occurrenceIndex);
					
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
	 * Prepare occurrences and occurrencesIndexes
	 */
	protected void prepareOccurences() {
		TIntIntIterator counts = this.supportCounts.iterator();
		int i = 0;
		
		while (counts.hasNext()) {
			counts.advance();
			this.occurrences[i] = counts.value();
			this.occurrencesIndexes.put(counts.key(), i);
			i += counts.value() + 1;
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
			this.next_index = -1;
			
			this.frequentItems = occurrencesIndexes.keys();
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
			final int candidateOccsIdx = occurrencesIndexes.get(candidate);
			
			for (int i=candidateIndex + 1; i < frequentItems.length; i++) {
				int j = frequentItems[i];
				
				if (supportCounts.get(j) >= candidateSupport) {
					int jOccsIdx = occurrencesIndexes.get(j);
					if (isAincludedInB(candidateOccsIdx, jOccsIdx)) {
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
		 * @return true if occurrences at aStart are included in occurrences at bStart
		 */
		private boolean isAincludedInB(final int aStart, final int bStart) {
			int aIt = aStart;
			int aMax = aIt + occurrences[aIt] + 1;
			int tidA = 0;
			
			int bIt = bStart;
			int bMax = bIt + occurrences[bIt] + 1;
			int tidB = 0;
			
			for (aIt++, bIt++; aIt < aMax && bIt < bMax; aIt++) {
				tidA = occurrences[aIt];
				tidB = occurrences[bIt];

				bIt++;
				while (tidB < tidA && bIt < bMax) {
					tidB = occurrences[bIt];
					bIt++;
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
