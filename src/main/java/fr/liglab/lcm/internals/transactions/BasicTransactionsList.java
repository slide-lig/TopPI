package fr.liglab.lcm.internals.transactions;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BasicTransactionsList extends TransactionsList {

	private List<TIntList> transactions = new ArrayList<TIntList>();
	
	@Override
	public int size() {
		return this.transactions.size();
	}
	
	@Override
	public String toString() {
		return this.transactions.toString();
	}
	
	@Override
	public Iterator<IterableTransaction> iterator() {
		final Iterator<TIntList> iter = this.transactions.iterator();
		return new Iterator<IterableTransaction>() {

			@Override
			public void remove() {
				iter.remove();
			}

			@Override
			public IterableTransaction next() {
				final TIntList l = iter.next();
				return new IterableTransaction() {

					@Override
					public TransactionIterator iterator() {
						return new TransComp(l);
					}
				};
			}

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}
		};
	}

	@Override
	public TransactionIterator get(int transaction) {
		if (transaction >= this.transactions.size()) {
			throw new IllegalArgumentException("transaction " + transaction + " does not exist");
		}
		return new TransComp(this.transactions.get(transaction));
	}

	@Override
	public TransactionsWriter getWriter() {
		return new TransactionsWriter() {
			private TIntList currentTransaction;

			@Override
			public void endTransaction() {
				transactions.add(currentTransaction);
				this.currentTransaction = null;
			}

			@Override
			public int beginTransaction(int support) {
				this.currentTransaction = new TIntArrayList();
				this.currentTransaction.add(support);
				return transactions.size();
			}

			@Override
			public void addItem(int item) {
				this.currentTransaction.add(item);
			}
		};
	}

	private class TransComp extends TransactionIterator {
		private final TIntList trans;
		private final TIntIterator transIter;
		private final int support;

		public TransComp(TIntList trans) {
			this.trans = trans;
			this.transIter = trans.iterator();
			this.support = transIter.next();
		}

		@Override
		public int next() {
			return transIter.next();
		}

		@Override
		public boolean hasNext() {
			return transIter.hasNext();
		}

		@Override
		public int getTransactionSupport() {
			return support;
		}

		@Override
		public void remove() {
			transIter.remove();
		}

		@Override
		public void setTransactionSupport(int s) {
			trans.set(0, s);
		}

	}
}
