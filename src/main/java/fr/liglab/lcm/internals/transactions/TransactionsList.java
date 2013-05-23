package fr.liglab.lcm.internals.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Stores transactions. Items in transactions are assumed to be sorted in
 * increasing order
 */
public abstract class TransactionsList implements Iterable<IterableTransaction> {

	abstract public TransactionIterator get(final int transaction);

	abstract public TransactionsWriter getWriter();

	/**
	 * @return how many IterableTransaction are behind this object
	 */
	abstract public int size();

	public void compress(final int prefixEnd) {
		List<IterableTransaction> sortList = new ArrayList<IterableTransaction>();
		for (IterableTransaction trans : this) {
			sortList.add(trans);
		}
		Collections.sort(sortList, new Comparator<IterableTransaction>() {

			@Override
			public int compare(IterableTransaction trans1, IterableTransaction trans2) {
				TransactionIterator t1 = trans1.iterator();
				TransactionIterator t2 = trans2.iterator();
				if (!t1.hasNext()) {
					if (t2.hasNext()) {
						return -1;
					} else {
						return 0;
					}
				}
				if (!t2.hasNext()) {
					return 1;
				}
				int t1Item = t1.next();
				int t2Item = t2.next();
				while (true) {
					if (t1Item > prefixEnd && t2Item > prefixEnd) {
						return 0;
					} else {
						if (t1Item < t2Item) {
							return -1;
						} else if (t1Item > t2Item) {
							return 1;
						} else {
							if (t1.hasNext()) {
								if (t2.hasNext()) {
									t1Item = t1.next();
									t2Item = t2.next();
									continue;
								} else {
									return 1;
								}
							} else {
								if (t2.hasNext()) {
									return -1;
								} else {
									return 0;
								}
							}
						}
					}
				}
			}

		});
		Iterator<IterableTransaction> transIter = sortList.iterator();
		IterableTransaction trans1 = transIter.next();
		while (transIter.hasNext()) {
			IterableTransaction trans2 = transIter.next();
			if (!this.merge(trans1, trans2, prefixEnd)) {
				trans1 = trans2;
			}
		}
	}

	private boolean merge(IterableTransaction trans1, IterableTransaction trans2, final int prefixEnd) {
		TransactionIterator t1 = trans1.iterator();
		TransactionIterator t2 = trans2.iterator();
		if (!t1.hasNext() || !t2.hasNext()) {
			return false;
		}
		int t1Item = t1.next();
		int t2Item = t2.next();
		while (true) {
			if (t1Item < prefixEnd) {
				if (t2Item < prefixEnd) {
					if (t1Item != t2Item) {
						return false;
					} else {
						if (t1.hasNext()) {
							t1Item = t1.next();
							if (t2.hasNext()) {
								t2Item = t2.next();
								continue;
							} else {
								if (t1Item < prefixEnd) {
									return false;
								} else {
									t1.remove();
									while (t1.hasNext()) {
										t1.next();
										t1.remove();
									}
									t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
									t2.setTransactionSupport(0);
									return true;
								}
							}
						} else {
							if (t2.hasNext()) {
								t2Item = t2.next();
								if (t2Item < prefixEnd) {
									return false;
								} else {
									t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
									t2.setTransactionSupport(0);
									return true;
								}
							} else {
								t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
								t2.setTransactionSupport(0);
								return true;
							}
						}
					}
				} else {
					return false;
				}
			} else {
				if (t2Item < prefixEnd) {
					return false;
				} else {
					break;
				}
			}
		}
		while (true) {
			if (t1Item == t2Item) {
				if (t1.hasNext()) {
					if (t2.hasNext()) {
						t1Item = t1.next();
						t2Item = t2.next();
						continue;
					} else {
						while (t1.hasNext()) {
							t1.next();
							t1.remove();
						}
						t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
						t2.setTransactionSupport(0);
						return true;
					}
				} else {
					t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
					t2.setTransactionSupport(0);
					return true;
				}
			} else {
				if (t1Item < t2Item) {
					t1.remove();
					if (t1.hasNext()) {
						t1Item = t1.next();
					} else {
						t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
						t2.setTransactionSupport(0);
						return true;
					}
				} else {
					if (t2.hasNext()) {
						t2Item = t2.next();
					} else {
						t1.remove();
						while (t1.hasNext()) {
							t1.next();
							t1.remove();
						}
						t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
						t2.setTransactionSupport(0);
						return true;
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		boolean first = true;
		for (IterableTransaction trans : this) {
			TransactionIterator iter = trans.iterator();
			if (first) {
				first = false;
			} else {
				sb.append("\n");
			}
			sb.append(iter.getTransactionSupport() + " {");
			while (iter.hasNext()) {
				sb.append(iter.next() + ",");
			}
			sb.append("}");
		}
		sb.append("]");
		return sb.toString();
	}

	public static void main(String[] args) {
		TransactionsList tl = new VIntConcatenatedTransactionsList(3, new int[] { 0, 3, 3, 2, 0, 3, 1, 2, 2 });
		// TransactionsList tl = new ConcatenatedTransactionsList(16, 3);
		TransactionsWriter w = tl.getWriter();
		System.out.println(Integer.MAX_VALUE - 3 + "");
		w.beginTransaction(Integer.MAX_VALUE - 3);
		w.addItem(1);
		w.addItem(2);
		w.addItem(3);
		w.addItem(5);
		w.addItem(6);
		w.addItem(8);
		w.endTransaction();
		w.beginTransaction(1);
		w.addItem(1);
		w.addItem(2);
		w.addItem(5);
		w.addItem(7);
		w.endTransaction();
		w.beginTransaction(3);
		w.addItem(1);
		w.addItem(2);
		w.addItem(3);
		w.addItem(5);
		w.addItem(7);
		w.addItem(8);
		w.endTransaction();
		System.out.println(tl);
		tl.compress(4);
		System.out.println(tl);
	}
}
