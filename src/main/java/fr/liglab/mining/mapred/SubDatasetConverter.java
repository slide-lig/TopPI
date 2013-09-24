package fr.liglab.mining.mapred;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.mining.internals.TransactionReader;
import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;

public class SubDatasetConverter implements Iterable<TransactionReader> {
	
	private Iterator<ConcatenatedTransactionsWritable> converted;
	private List<int[]> cached;
	
	public SubDatasetConverter(Iterator<ConcatenatedTransactionsWritable> from) {
		this.converted = from;
		this.cached = new ArrayList<int[]>();
	}
	
	@Override
	public Iterator<TransactionReader> iterator() {
		if (this.converted == null) {
			return new ArrayConcatenator(this.cached.iterator());
		} else {
			WritableDecorator decorated = new WritableDecorator(this.converted);
			this.converted = null;
			return new ArrayConcatenator(decorated);
		}
	}
	
	private class WritableDecorator implements Iterator<int[]> {
		
		private final Iterator<ConcatenatedTransactionsWritable> decorated;
		
		public WritableDecorator(Iterator<ConcatenatedTransactionsWritable> d) {
			this.decorated = d;
		}
		
		@Override
		public boolean hasNext() {
			return this.decorated.hasNext();
		}

		@Override
		public int[] next() {
			int[] array = this.decorated.next().get();
			cached.add(array);
			return array;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}
		
	}
	
	private class ArrayConcatenator implements Iterator<TransactionReader> {
		
		private final Iterator<int[]> source;
		private final ArrayReader reader = new ArrayReader();
		private int[] concatenated = null;
		private int next = 0;
		
		public ArrayConcatenator(Iterator<int[]> arrays) {
			this.source = arrays;
		}
		
		@Override
		public boolean hasNext() {
			return this.source.hasNext() || this.next < this.concatenated.length;
		}

		@Override
		public TransactionReader next() {
			if (this.concatenated == null || this.next == this.concatenated.length) {
				this.concatenated = this.source.next();
				this.next = 0;
			}
			
			final int start = this.next + 1;
			this.next += this.concatenated[this.next];
			
			this.reader.recycle(this.concatenated, start, this.next++);
			
			return this.reader;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}
		
	}
	
	private class ArrayReader implements TransactionReader {
		
		private int[] transaction = null;
		private int i = 0;
		private int last = -1;
		
		/**
		 * @param source
		 * @param from first valid index in source
		 * @param to last valid index in source
		 */
		public void recycle(int[] source, int from, int to) {
			this.transaction = source;
			this.i = from;
			this.last = to;
		}

		@Override
		public int next() {
			return this.transaction[this.i++];
		}

		@Override
		public boolean hasNext() {
			return this.i <= this.last;
		}

		@Override
		public int getTransactionSupport() {
			return 1;
		}
	}
}
