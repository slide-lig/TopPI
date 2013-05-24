package fr.liglab.lcm.internals.transactions;

import fr.liglab.lcm.internals.nomaps.Counters;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BasicTransactionsList extends TransactionsList {
	public static boolean compatible(Counters c) {
		return true;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	private List<TIntList> transactions = new ArrayList<TIntList>();

	@Override
	public TransactionsList clone() {
		BasicTransactionsList o = (BasicTransactionsList) super.clone();
		o.transactions = new ArrayList<TIntList>(this.transactions.size());
		for (TIntList l : this.transactions) {
			o.transactions.add(new TIntArrayList(l));
		}
		return o;
	}

	@Override
	public int size() {
		return this.transactions.size();
	}

	@Override
	public Iterator<IterableTransaction> iterator() {
		final Iterator<TIntList> iter = this.transactions.iterator();
		return new Iterator<IterableTransaction>() {
			private TIntList next = findNext();

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			private TIntList findNext() {
				while (true) {
					if (!iter.hasNext()) {
						return null;
					} else {
						TIntList l = iter.next();
						if (l.get(0) != 0) {
							return l;
						}

					}
				}
			}

			@Override
			public IterableTransaction next() {
				final TIntList l = this.next;
				this.next = findNext();
				return new IterableTransaction() {

					@Override
					public TransactionIterator iterator() {
						return new TransComp(l);
					}
				};
			}

			@Override
			public boolean hasNext() {
				return this.next != null;
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

	private class TransComp implements TransactionIterator {
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
