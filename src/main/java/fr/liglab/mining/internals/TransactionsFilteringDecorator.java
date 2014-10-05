package fr.liglab.mining.internals;

import java.util.Iterator;

import fr.liglab.mining.internals.TransactionReader;

/**
 * Decorates a transactions iterator : transactions are filtered (and, maybe, rebased) 
 * as they're read.
 */
class TransactionsFilteringDecorator implements Iterator<TransactionReader> {

	protected final Iterator<TransactionReader> wrapped;
	protected final int[] support;
	protected FilteredTransaction instance;
	protected final boolean arrayIsTooShort;

	/**
	 * @param filtered
	 * @param supportCounts items having null or negative values will be filtered
	 */
	public TransactionsFilteringDecorator(Iterator<TransactionReader> filtered, int[] supportCounts) {
		this(filtered, supportCounts, false);
	}
	
	public TransactionsFilteringDecorator(Iterator<TransactionReader> filtered, int[] supportCounts, boolean arrayIsTooShort) {
		this.wrapped = filtered;
		this.instance = null;
		this.support = supportCounts;
		this.arrayIsTooShort = arrayIsTooShort;
	}

	@Override
	public final boolean hasNext() {
		return this.wrapped.hasNext();
	}

	@Override
	public TransactionReader next() {
		if (this.instance == null) {
			if (this.arrayIsTooShort) {
				this.instance = new SaferFilteredTransaction(this.wrapped.next());
			} else {
				this.instance = new FilteredTransaction(this.wrapped.next());
			}
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
				if (support[this.next] > 0) {
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
	
	protected class SaferFilteredTransaction extends FilteredTransaction {

		public SaferFilteredTransaction(TransactionReader filtered) {
			super(filtered);
		}
		
		@Override
		protected void findNext() {
			while (this.wrapped.hasNext()) {
				this.next = this.wrapped.next();
				if (this.next < support.length && support[this.next] > 0) {
					this.hasNext = true;
					return;
				}
			}

			this.hasNext = false;
		}
	}
}
