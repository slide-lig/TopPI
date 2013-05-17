package fr.liglab.lcm.internals.nomaps;

import java.util.Arrays;
import java.util.Iterator;

import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.util.ItemsetsFactory;

/**
 * Decorates a transactions iterator : items in contained transactions will be :
 *  - renamed 
 *  - filtered if renaming map gives -1 for their ID
 *  - sorted in ascending order
 */
class TransactionsRenamerFilterSorter implements Iterator<TransactionReader> {
	
	protected final Iterator<TransactionReader> wrapped;
	protected final int[] newNames;
	protected final FilteredSortedTransaction instance =  new FilteredSortedTransaction();
	
	public TransactionsRenamerFilterSorter(Iterator<TransactionReader> filtered, int[] renaming) {
		this.wrapped = filtered;
		this.newNames = renaming;
	}
	
	/**
	 * FIXME maybe some transactions will be empty once filtered !
	 */
	@Override
	public final boolean hasNext() {
		return this.wrapped.hasNext();
	}
	
	@Override
	public final TransactionReader next() {
		this.instance.reset(this.wrapped.next());
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
		
		public void reset(TransactionReader filtered) {
			while(filtered.hasNext()) {
				final int item = newNames[filtered.next()];
				if (item >= 0) {
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
