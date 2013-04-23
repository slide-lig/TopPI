package fr.liglab.lcm.util;

import java.util.ArrayList;
import java.util.Iterator;

import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

/**
 * Translates an Iterator<TransactionWritable> to a  Iterator<TransactionWritable>
 * AND keeps a copy of all transactions in order to allow a second pass on the dataset
 */
public final class ReIterableTransactionWritable implements Iterator<TransactionReader> {
	private final Iterator<TransactionWritable> wrapped;
	private final ArrayTranslator translator = new ArrayTranslator();
	private final ArrayList<int[]> copy = new ArrayList<int[]>();
	
	public ReIterableTransactionWritable(Iterator<TransactionWritable> original) {
		this.wrapped = original;
	}
	
	public Iterator<TransactionReader> reIterate() {
		return new CopyIterator(this.copy.iterator());
	}

	@Override
	public boolean hasNext() {
		return this.wrapped.hasNext();
	}

	@Override
	public TransactionReader next() {
		int[] next = this.wrapped.next().get();
		this.translator.setTransaction(next);
		this.copy.add(next);
		return this.translator;
	}

	@Override
	public void remove() {
		this.wrapped.remove();
	}
	
	public final class ArrayTranslator implements TransactionReader {
		private int[] transaction;
		private int i;
		
		@Override public int getTransactionSupport() { return 1; }
		@Override public int next() { return this.transaction[this.i++]; }
		@Override public boolean hasNext() { return this.i < this.transaction.length; }
		
		void setTransaction(int[] val) {
			this.transaction = val;
			this.i = 0;
		}
	}
	
	private final class CopyIterator implements Iterator<TransactionReader> {
		private final ArrayTranslator translator = new ArrayTranslator();
		private final Iterator<int[]> wrapped;
		
		public CopyIterator(Iterator<int[]> original) {
			this.wrapped = original;
		}
		
		@Override
		public boolean hasNext() {
			return this.wrapped.hasNext();
		}

		@Override
		public TransactionReader next() {
			this.translator.setTransaction(this.wrapped.next());
			return this.translator;
		}

		@Override
		public void remove() {
			this.wrapped.remove();
		}
	}
}
