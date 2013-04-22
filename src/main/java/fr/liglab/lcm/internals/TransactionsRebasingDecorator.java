package fr.liglab.lcm.internals;

import java.util.Arrays;
import java.util.Iterator;

import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.map.TIntIntMap;

/**
 * Decorates a transactions iterator : transactions will be filtered, rebased and sorted as they're read
 */
class TransactionsRebasingDecorator implements Iterator<TransactionReader> {
	
	protected final Iterator<TransactionReader> wrapped;
	protected final TIntIntMap rebasing;
	protected FilteredSortedRebasedTransaction instance;
	
	public TransactionsRebasingDecorator(Iterator<TransactionReader> filtered, TIntIntMap newBase) {
		this.wrapped = filtered;
		this.rebasing = newBase;
		this.instance = null;
	}
	
	@Override
	public final boolean hasNext() {
		return this.wrapped.hasNext();
	}
	
	@Override
	public TransactionReader next() {
		if (this.instance == null) {
			this.instance = new FilteredSortedRebasedTransaction(this.wrapped.next());
		} else {
			this.instance.reset(this.wrapped.next());
		}
		
		return this.instance;
	}
	
	@Override
	public final void remove() {
		this.wrapped.remove();
	}
	
	
	protected class FilteredSortedRebasedTransaction implements TransactionReader {
		
		protected final ItemsetsFactory builder = new ItemsetsFactory();
		
		protected int[] filteredRebasedSorted;
		protected int next;
		protected int weight;
		
		public FilteredSortedRebasedTransaction(TransactionReader filtered) {
			this.reset(filtered);
		}
		
		public void reset(TransactionReader filtered) {
			while(filtered.hasNext()) {
				final int item = filtered.next();
				if (rebasing.containsKey(item)) {
					builder.add(rebasing.get(item));
				}
			}
			
			filteredRebasedSorted = builder.get();
			Arrays.sort(filteredRebasedSorted);
			
			this.next = 0;
			this.weight = filtered.getTransactionSupport();
		}

		@Override
		public final int getTransactionSupport() {
			return this.weight;
		}

		@Override
		public final int next() {
			final int value = this.filteredRebasedSorted[this.next];
			this.next++;
			return value;
		}

		@Override
		public final boolean hasNext() {
			return this.next < this.filteredRebasedSorted.length;
		}
	}
}
