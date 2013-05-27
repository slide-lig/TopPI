package fr.liglab.lcm.internals.transactions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import fr.liglab.lcm.internals.Counters;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

public class BasicTransactionsList extends TransactionsList {
	public static boolean compatible(Counters c) {
		return true;
	}

	public static int getMaxTransId(Counters c) {
		return c.distinctTransactionsCount - 1;
	}

	private List<TIntList> transactions = new ArrayList<TIntList>();
	private int size = 0;

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
		return this.size;
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
					private TransComp iter = getIterator();

					@Override
					public TransactionIterator iterator() {
						iter.set(l);
						return iter;
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
	public TIntIterator getIdIterator() {
		return new IdIter();
	}

	@Override
	public TransComp getIterator() {
		return new TransComp();
	}

	public void positionIterator(int transaction, TransComp iter) {
		if (transaction >= this.transactions.size()) {
			throw new IllegalArgumentException("transaction " + transaction + " does not exist");
		}
		iter.set(this.transactions.get(transaction));
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
				if (support != 0) {
					size++;
				}
				return transactions.size();
			}

			@Override
			public void addItem(int item) {
				this.currentTransaction.add(item);
			}
		};
	}

	private class TransComp implements ReusableTransactionIterator {
		private TIntList trans;
		private TIntIterator transIter;
		private int support;

		public TransComp() {

		}

		@Override
		public void setTransaction(int transaction) {
			positionIterator(transaction, this);
		}

		public void set(TIntList trans) {
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
			if (this.support == 0 && s != 0) {
				size++;
			} else if (this.support != 0 && s == 0) {
				size--;
			}
			trans.set(0, s);
			this.support = s;
		}

	}

	private class IdIter implements TIntIterator {
		private int pos;
		private int nextPos = -1;

		public IdIter() {
			this.findNext();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int next() {
			this.pos = this.nextPos;
			this.findNext();
			return this.pos;
		}

		private void findNext() {
			while (true) {
				this.nextPos++;
				if (nextPos >= transactions.size()) {
					this.nextPos = -1;
					return;
				}
				if (transactions.get(this.nextPos).get(0) != 0) {
					return;
				}
			}
		}

		@Override
		public boolean hasNext() {
			return this.nextPos != -1;
		}
	}

}
