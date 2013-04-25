package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIterator;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

/**
 * A reader for transactions concatenated in an int[]
 */
final class ConcatenatedTransactionsReader implements Iterator<TransactionReader> {
	
	private final int[] concatenated;
	private final TIntIterator indexes;
	private final Reader reader = new Reader();
	private final boolean readWeights;
	
	ConcatenatedTransactionsReader(int[] source, TIntIterator occurrencesIndexes, 
			boolean readTransactionsWeights) {
		
		this.concatenated = source;
		this.indexes = occurrencesIndexes;
		this.readWeights = readTransactionsWeights;
	}
	
	@Override
	public TransactionReader next() {
		this.reader.setCursor(this.indexes.next());
		return this.reader;
	}
	
	@Override public boolean hasNext() { return this.indexes.hasNext(); }
	@Override public void remove() { throw new NotImplementedException(); }
	
	
	private final class Reader implements TransactionReader {
		private int i = 0;
		private int max = 0;
		private int weight = 1;
		
		void setCursor(int at) {
			this.i = at+1;
			this.max = this.i + concatenated[at];
			
			if (readWeights) {
				weight = concatenated[at-1];
			}
		}
		
		@Override public int getTransactionSupport() { return this.weight; }
		@Override public int next() { return concatenated[this.i++]; }
		@Override public boolean hasNext() { return this.i < this.max; }
	}
}
