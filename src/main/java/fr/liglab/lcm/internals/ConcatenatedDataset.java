package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.util.Iterator;

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
		
		TIntSet retained = this.supportCounts.keySet();
		this.concatenated = new int[remainingItemsCount + this.transactionsCount];
		int i = 1;
		int tIndex = 0;
		
		for (int[] transaction : transactionsCopier) {
			int length = 0;
			
			for (int item : transaction) {
				if (retained.contains(item)) {
					this.concatenated[i] = item;
					this.occurrences.get(item).add(tIndex);
					length++;
					i++;
				}
			}
			
			this.concatenated[tIndex] = length;
			tIndex = i;
			i++;
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
		
		int remainingItemsCount = genClosureAndFilterCount();
		this.prepareOccurences();
		
		TIntSet retained = this.supportCounts.keySet();
		this.concatenated = new int[remainingItemsCount + this.transactionsCount];
		int i = 1;
		int tIndex = 0;
		
		iterator = occurrences.iterator();
		while(iterator.hasNext()) {
			int tid = iterator.next();
			int l = parent.concatenated[tid];
			int length = 0;
			
			for (int j = tid + 1; i <= tid+l; i++) {
				int item = parent.concatenated[j];
				
				if (retained.contains(item)) {
					this.concatenated[i] = item;
					this.occurrences.get(item).add(tIndex);
					length++;
					i++;
				}
			}
			
			this.concatenated[tIndex] = length;
			tIndex = i;
			i++;
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
		// TODO Auto-generated method stub
		return null;
	}

}
