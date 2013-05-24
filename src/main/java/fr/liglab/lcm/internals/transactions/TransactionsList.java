package fr.liglab.lcm.internals.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import fr.liglab.lcm.internals.nomaps.Counters;

/**
 * Stores transactions. Items in transactions are assumed to be sorted in
 * increasing order
 */
public abstract class TransactionsList implements Iterable<IterableTransaction> {

	@Override
	public TransactionsList clone() {
		try {
			return (TransactionsList) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return null;
	}

	abstract public TransactionIterator get(final int transaction);

	abstract public TransactionsWriter getWriter();

	/**
	 * @return how many IterableTransaction are behind this object
	 */
	abstract public int size();

	public void compress(final int prefixEnd) {
		compress(prefixEnd, null);
	}

	public void compress(final int prefixEnd, Counters c) {
		List<IterableTransaction> sortList = new ArrayList<IterableTransaction>();
		for (IterableTransaction trans : this) {
			sortList.add(trans);
		}
		Collections.sort(sortList, new TransactionsComparator(prefixEnd));
		Iterator<IterableTransaction> transIter = sortList.iterator();
		IterableTransaction trans1 = transIter.next();
		while (transIter.hasNext()) {
			IterableTransaction trans2 = transIter.next();
			if (!this.merge(trans1, trans2, prefixEnd, c)) {
				trans1 = trans2;
			}
		}
	}

	private boolean merge(IterableTransaction trans1, IterableTransaction trans2, final int prefixEnd, Counters c) {
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
									if (c != null) {
										c.decrementCounts(t1Item, t1.getTransactionSupport());
									}
									while (t1.hasNext()) {
										t1Item = t1.next();
										t1.remove();
										if (c != null) {
											c.decrementCounts(t1Item, t1.getTransactionSupport());
										}
									}
									t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
									if (c != null) {
										c.decrementTrans(t2.getTransactionSupport());
									}
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
									if (c != null) {
										c.decrementCounts(t2Item, t2.getTransactionSupport());
										while (t2.hasNext()) {
											t2Item = t2.next();
											c.decrementCounts(t2Item, t2.getTransactionSupport());
										}
									}
									t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
									if (c != null) {
										c.decrementTrans(t2.getTransactionSupport());
									}
									t2.setTransactionSupport(0);
									return true;
								}
							} else {
								t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
								if (c != null) {
									c.decrementTrans(t2.getTransactionSupport());
								}
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
							t1Item = t1.next();
							t1.remove();
							if (c != null) {
								c.decrementCounts(t1Item, t1.getTransactionSupport());
							}
						}
						t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
						if (c != null) {
							c.decrementTrans(t2.getTransactionSupport());
						}
						t2.setTransactionSupport(0);
						return true;
					}
				} else {
					if (c != null) {
						while (t2.hasNext()) {
							t2Item = t2.next();
							c.decrementCounts(t2Item, t2.getTransactionSupport());
						}
					}
					t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
					if (c != null) {
						c.decrementTrans(t2.getTransactionSupport());
					}
					t2.setTransactionSupport(0);
					return true;
				}
			} else {
				if (t1Item < t2Item) {
					t1.remove();
					if (c != null) {
						c.decrementCounts(t1Item, t1.getTransactionSupport());
					}
					if (t1.hasNext()) {
						t1Item = t1.next();
					} else {
						if (c != null) {
							c.decrementCounts(t2Item, t2.getTransactionSupport());
							while (t2.hasNext()) {
								t2Item = t2.next();
								c.decrementCounts(t2Item, t2.getTransactionSupport());
							}
						}
						t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
						if (c != null) {
							c.decrementTrans(t2.getTransactionSupport());
						}
						t2.setTransactionSupport(0);
						return true;
					}
				} else {
					if (t2.hasNext()) {
						t2Item = t2.next();
					} else {
						t1.remove();
						if (c != null) {
							c.decrementCounts(t1Item, t1.getTransactionSupport());
						}
						while (t1.hasNext()) {
							t1Item = t1.next();
							t1.remove();
							if (c != null) {
								c.decrementCounts(t1Item, t1.getTransactionSupport());
							}
						}
						t1.setTransactionSupport(t1.getTransactionSupport() + t2.getTransactionSupport());
						if (c != null) {
							c.decrementTrans(t2.getTransactionSupport());
						}
						t2.setTransactionSupport(0);
						return true;
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(this.size() + " transactions\n[");
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

	private static class TransactionsComparator implements Comparator<IterableTransaction> {
		private int prefixEnd;

		private TransactionsComparator(int prefixEnd) {
			this.prefixEnd = prefixEnd;
		}

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
	}

	public static void main(String[] args) {
		int[] freqs = new int[Short.MAX_VALUE * 2 + 1];
		freqs[1] = 3;
		freqs[2] = 3;
		freqs[3] = 2;
		freqs[5] = 3;
		freqs[Short.MAX_VALUE * 2 - 2] = 1;
		freqs[Short.MAX_VALUE * 2 - 1] = 2;
		freqs[Short.MAX_VALUE * 2] = 2;
		// TransactionsList tl = new VIntConcatenatedTransactionsList(3, freqs);
		TransactionsList tl = new IntIndexedTransactionsList(16, 3);
		// TransactionsList tl = new ConcatenatedTransactionsList(16, 3);
		// TransactionsList tl = new BasicTransactionsList();
		TransactionsWriter w = tl.getWriter();
		w.beginTransaction(Short.MAX_VALUE + 3);
		w.addItem(1);
		w.addItem(2);
		w.addItem(3);
		w.addItem(5);
		w.addItem(Short.MAX_VALUE * 2 - 2);
		w.addItem(Short.MAX_VALUE * 2);
		w.endTransaction();
		w.beginTransaction(1);
		w.addItem(1);
		w.addItem(2);
		w.addItem(5);
		w.addItem(Short.MAX_VALUE * 2 - 1);
		w.endTransaction();
		w.beginTransaction(3);
		w.addItem(1);
		w.addItem(2);
		w.addItem(3);
		w.addItem(5);
		w.addItem(Short.MAX_VALUE * 2 - 1);
		w.addItem(Short.MAX_VALUE * 2);
		w.endTransaction();
		System.out.println(tl);
		tl.compress(4);
		System.out.println(tl);
	}
}
