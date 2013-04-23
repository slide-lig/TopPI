package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

/**
 * In this dataset (which does implement occurrence-delivery) transactions are 
 * prefixed by their length and concatenated in a single int[] 
 * 
 * It does NOT stores transactions' weights
 * 
 * It does NOT override ppTest (although it could easily) coz it's made for 
 * really sparse datasets.
 */
class ConcatenatedDataset extends Dataset {

	protected final DatasetCounters counters;
	protected final int[] concatenated;
	
	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurrences-arrays.
	 */
	protected final TIntObjectHashMap<TIntArrayList> occurrences = new TIntObjectHashMap<TIntArrayList>();
	
	ConcatenatedDataset(final DatasetCounters counts,
			final Iterator<TransactionReader> transactions) {
		
		this.counters = counts;
		
		int tableLength = counts.supportsSum + counts.transactionsCount;
		this.concatenated = new int[tableLength];
		
		// prepare occurrences lists
		TIntIntIterator supports = counts.supportCounts.iterator();
		while (supports.hasNext()) {
			supports.advance();
			this.occurrences.put(supports.key(), new TIntArrayList(supports.value()));
		}
		
		// COPY
		int tIdx = 0;
		int tLen = 0;
		int i = 1;
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			while (transaction.hasNext()) {
				this.concatenated[i++] = transaction.next();
				tLen++;
			}
			this.concatenated[tIdx] = tLen;
			tIdx = i;
		}
	}
	
	
	@Override
	public DatasetCounters getCounters() {
		return this.counters;
	}
	
	@Override
	public Iterator<TransactionReader> getSupport(int item) {
		return new OccurrencesIterator(this.occurrences.get(item));
	}
	
	
	protected final class OccurrencesIterator implements Iterator<TransactionReader> {
		private final TIntIterator indexes;
		private final Reader reader = new Reader();
		
		OccurrencesIterator(TIntArrayList occurrencesIndexes) {
			this.indexes = occurrencesIndexes.iterator();
		}
		
		@Override
		public TransactionReader next() {
			this.reader.setCursor(this.indexes.next());
			return this.reader;
		}
		
		@Override public boolean hasNext() { return this.indexes.hasNext(); }
		@Override public void remove() { throw new NotImplementedException(); }
	}
	
	
	protected final class Reader implements TransactionReader {
		private int i = 0;
		private int max = 0;
		
		void setCursor(int at) {
			this.i = at+1;
			this.max = at + concatenated[at];
		}
		
		@Override public int getTransactionSupport() { return 1; }
		@Override public int next() { return concatenated[this.i++]; }
		@Override public boolean hasNext() { return this.i <= this.max; }
	}
}
