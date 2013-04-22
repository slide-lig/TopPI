package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.set.TIntSet;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Decorates a transactions iterator : transactions will be filtered and sorted as they're read.
 */
class TransactionsSortingDecorator implements Iterator<TransactionReader> {
	
	protected final Iterator<TransactionReader> wrapped;
	protected final TIntSet keeped;
	protected FilteredSortedTransaction instance;
	
	public TransactionsSortingDecorator(Iterator<TransactionReader> filtered, TIntSet keepedItems) {
		this.wrapped = filtered;
		this.keeped = keepedItems;
		this.instance = null;
	}
	
	@Override
	public final boolean hasNext() {
		return this.wrapped.hasNext();
	}
	
	@Override
	public TransactionReader next() {
		if (this.instance == null) {
			this.instance = new FilteredSortedTransaction(this.wrapped.next());
		} else {
			this.instance.reset(this.wrapped.next());
		}
		
		return this.instance;
	}
	
	@Override
	public final void remove() {
		this.wrapped.remove();
	}
	
	
	protected class FilteredSortedTransaction implements TransactionReader {
		
		protected final ItemsetsFactory builder = new ItemsetsFactory();
		
		protected int[] filteredSorted;
		protected int next;
		protected int weight;
		
		public FilteredSortedTransaction(TransactionReader filtered) {
			this.reset(filtered);
		}
		
		public void reset(TransactionReader filtered) {
			while(filtered.hasNext()) {
				final int item = filtered.next();
				if (keeped.contains(item)) {
					builder.add(item);
				}
			}
			
			filteredSorted = builder.get();
			Arrays.sort(filteredSorted);
			
			this.next = 0;
			this.weight = filtered.getTransactionSupport();
		}

		@Override
		public final int getTransactionSupport() {
			return this.weight;
		}

		@Override
		public final int next() {
			final int value = this.filteredSorted[this.next];
			this.next++;
			return value;
		}

		@Override
		public final boolean hasNext() {
			return this.next < this.filteredSorted.length;
		}
	}
}
