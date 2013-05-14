package fr.liglab.lcm.internals.nomaps;

import java.util.Iterator;

import fr.liglab.lcm.internals.TransactionReader;

/**
 * Decorates a transactions iterator : transactions are filtered (and, maybe, rebased) 
 * as they're read.
 */
class TransactionsFilteringDecorator implements Iterator<TransactionReader> {

	protected final Iterator<TransactionReader> wrapped;
	protected final int[] itemSupport;
	protected final int[] rebasing;
	protected FilteredTransaction instance;

	/**
	 * @param filtered
	 * @param itemSupport - only items having a positive value will be kept from "filtered"
	 * @param rebasing may be null, in which case no rebasing happens
	 */
	public TransactionsFilteringDecorator(Iterator<TransactionReader> filtered, int[] itemSupport, int[] rebasing) {
		this.wrapped = filtered;
		this.itemSupport = itemSupport;
		this.instance = null;
		this.rebasing = rebasing;
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
				if (itemSupport[this.next] > 0) {
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
			if (rebasing != null) {
				return rebasing[value];
			} else {
				return value;
			}
		}

		@Override
		public final boolean hasNext() {
			return this.hasNext;
		}
	}
}
