package fr.liglab.lcm.internals.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class TransactionsCompressor {
	private final int prefixEnd;

	public TransactionsCompressor(int prefixEnd) {
		this.prefixEnd = prefixEnd;
	}

	public void compress(TransactionsList tl) {
		if (!tl.isSorted()) {
			throw new IllegalArgumentException("each transaction must be sorted before a compression");
		}
		List<IterableTransaction> sortList = new ArrayList<IterableTransaction>();
		for (IterableTransaction trans : tl) {
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
			if (!this.merge(trans1, trans2)) {
				trans1 = trans2;
			}
		}
	}

	private boolean merge(IterableTransaction trans1, IterableTransaction trans2) {
		TransactionIterator t1 = trans1.iterator();
		TransactionIterator t2 = trans2.iterator();
		if (!t1.hasNext() || !t2.hasNext()) {
			return false;
		}
		int t1Item = t1.next();
		int t2Item = t2.next();
		while (true) {
			if (t1Item < this.prefixEnd) {
				if (t2Item < this.prefixEnd) {
					if (t1Item != t2Item) {
						return false;
					} else {
						if (t1.hasNext()) {
							t1Item = t1.next();
							if (t2.hasNext()) {
								t2Item = t2.next();
								continue;
							} else {
								if (t1Item < this.prefixEnd) {
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
								if (t2Item < this.prefixEnd) {
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
				if (t2Item < this.prefixEnd) {
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
						t1.next();
						t2.next();
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
}
