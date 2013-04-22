package fr.liglab.lcm.internals;

import gnu.trove.set.TIntSet;

import java.util.Iterator;

/**
 * Decorates a transactions iterator : transactions are filtered as they're read.
 */
class TransactionsFilteringDecorator implements Iterator<TransactionReader> {
	
	protected final Iterator<TransactionReader> wrapped;
	protected final TIntSet keeped;
	protected FilteredTransaction instance;
	
	public TransactionsFilteringDecorator(Iterator<TransactionReader> filtered, TIntSet keepedItems) {
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
			this.instance = new FilteredTransaction(this.wrapped.next());
		} else {
			this.instance.reset(this.wrapped.next());
		}
		
		return this.instance;
	}
	
	@Override
	public final void remove() {
		this.wrapped.remove();
	}
	
	protected class FilteredTransaction implements TransactionReader {
		
		protected TransactionReader wrapped;
		protected int next;
		protected boolean hasNext;
		
		public FilteredTransaction(TransactionReader filtered) {
			this.reset(filtered);
		}
		
		public void reset(TransactionReader filtered) {
			this.wrapped = filtered;
			this.next = 0;
			
			this.findNext();
		}

		protected void findNext() {
			while (this.wrapped.hasNext()) {
				this.next = this.wrapped.next();
				if (keeped.contains(this.next)) {
					this.hasNext = true;
					return;
				}
			}
			
			this.hasNext = false;
		}

		@Override
		public final int getTransactionSupport() {
			return this.wrapped.getTransactionSupport();
		}

		@Override
		public final int next() {
			final int value = this.next;
			this.findNext();
			return value;
		}

		@Override
		public final boolean hasNext() {
			return this.hasNext;
		}
	}
}
