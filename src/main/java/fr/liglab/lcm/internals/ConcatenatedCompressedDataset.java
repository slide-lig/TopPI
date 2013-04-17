package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Here all transactions are prefixed by their length and concatenated in a
 * single int[]
 * 
 * This dataset internally performs - basic reduction : transactions contain
 * only items having a support count in [minusp, 100% [ - occurrence delivery :
 * occurrences are stored as indexes in the concatenated array - fast
 * prefix-preserving test (see inner class CandidatesIterator)
 */
public class ConcatenatedCompressedDataset extends FilteredDataset {
	// TODO extract compression and make it generic using the reader API
	/**
	 * Support list will contain transactions with 0 frequency because
	 * compression occurs AFTER their construction
	 */
	protected int[] concatenated;
	private TIntList transactionsList;

	public ConcatenatedCompressedDataset(int minimumsupport, Iterator<int[]> transactions)
			throws DontExploreThisBranchException {
		super(minimumsupport, transactions);
		this.compress(this.transactionsList);
		this.transactionsList = null;
	}

	public ConcatenatedCompressedDataset(IterableDataset parent, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(parent, extension, ignoreItems);
		this.compress(this.transactionsList);
		this.transactionsList = null;
	}

	public ConcatenatedCompressedDataset(IterableDataset parent, int extension) throws DontExploreThisBranchException {
		super(parent, extension);
		this.compress(this.transactionsList);
		this.transactionsList = null;
	}

	@Override
	protected void prepareTransactionsStructure(int sumOfRemainingItemsSupport, int distinctTransactionsLength,
			int distinctTransactionsCount) {
		this.transactionsList = new TIntArrayList(this.transactionsCount);
		int maxItemSize = Math.min(sumOfRemainingItemsSupport, distinctTransactionsLength);
		this.concatenated = new int[maxItemSize + 2 * distinctTransactionsCount];
	}

	@Override
	public int getRealSize() {
		return this.concatenated.length * (Integer.SIZE / Byte.SIZE);
	}

	@Override
	public Dataset createUnfilteredDataset(FilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedCompressedDataset(upper, extension, ignoreItems);
	}

	@Override
	public Dataset createFilteredDataset(FilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		return new ConcatenatedCompressedDataset(upper, extension, ignoreItems);
	}

	@Override
	protected TransactionsWriter getTransactionsWriter(boolean sourceSorted) {
		return new TransWriter(sourceSorted);
	}

	@Override
	public boolean itemsSorted() {
		return true;
	}

	@Override
	protected TransactionReader readTransaction(int tid) {
		return new ItemsIterator(tid);
	}

	private void compress(TIntList transactionsList) {
		if (!transactionsList.isEmpty()) {
			ArrayList<Integer> sortedList = new ArrayList<Integer>(transactionsList.size());
			TIntIterator iter = transactionsList.iterator();
			while (iter.hasNext()) {
				sortedList.add(iter.next());
			}
			Collections.sort(sortedList, new Comparator<Integer>() {

				public int compare(Integer t1, Integer t2) {
					if (concatenated[t1 - 1] == 0 || concatenated[t2 - 1] == 0) {
						throw new RuntimeException("transaction with frequency 0, should have been eliminated");
					} else {
						int t1length = concatenated[t1];
						int t2length = concatenated[t2];
						int iterationLength = Math.min(t1length, t2length);
						for (int i = 1; i <= iterationLength; i++) {
							if (concatenated[t1 + i] < concatenated[t2 + i]) {
								return -1;
							} else if (concatenated[t1 + i] > concatenated[t2 + i]) {
								return 1;
							}
						}
						return t1length - t2length;
					}
				}
			});
			int currentRef = sortedList.get(0);
			int currentRefLength = concatenated[currentRef];
			for (int i = 1; i < sortedList.size(); i++) {
				int trans = sortedList.get(i);
				int transLength = concatenated[trans];
				if (transLength == 0) {
					break;
				}
				int pointerCurrentRef = 1;
				int pointerTrans = 1;
				final int prefixEnd = Integer.MAX_VALUE;
				while (true) {
					if (pointerCurrentRef > currentRefLength) {
						if (pointerTrans > transLength || this.concatenated[trans + pointerTrans] > prefixEnd) {
							// prefix comparison finished, we already took care
							// of
							// the suffix of the reference, and we discard the
							// rest
							// of the suffix of trans
							this.concatenated[currentRef - 1] += this.concatenated[trans - 1];
							this.concatenated[trans - 1] = 0;
							break;
						} else {
							// prefix comparison not finished, so different
							// prefix,
							// switch to next
							currentRef = trans;
							currentRefLength = transLength;
							break;
						}
					} else if (pointerTrans > transLength) {
						if (this.concatenated[currentRef + pointerCurrentRef] > prefixEnd) {
							// same prefix, but all remaining suffix goes away
							Arrays.fill(this.concatenated, -1, currentRef + pointerCurrentRef, currentRef
									+ currentRefLength + 1);
							this.concatenated[currentRef - 1] += this.concatenated[trans - 1];
							this.concatenated[trans - 1] = 0;
							break;
						} else {
							// prefix comparison not finished, so different
							// prefix,
							// switch to next
							currentRef = trans;
							currentRefLength = transLength;
							break;
						}
					} else if (concatenated[currentRef + pointerCurrentRef] == concatenated[trans + pointerTrans]) {
						// same value, doesn't matter if we're in prefix or
						// suffix,
						// go on
						pointerTrans++;
						do {
							pointerCurrentRef++;
						} while (pointerCurrentRef <= currentRefLength
								&& this.concatenated[currentRef + pointerCurrentRef] == -1);
					} else if (concatenated[currentRef + pointerCurrentRef] < prefixEnd
							|| concatenated[trans + pointerTrans] < prefixEnd) {
						// prefix comparison not finished, so different prefix,
						// switch to next
						currentRef = trans;
						currentRefLength = transLength;
						break;
					}
					// from that point, suffix match, so they will be merged
					// eventually
					else if (concatenated[currentRef + pointerCurrentRef] > concatenated[trans + pointerTrans]) {
						// skip the suffix of trans that is not in ref
						do {
							pointerTrans++;
						} while (pointerTrans <= transLength
								&& this.concatenated[trans + pointerTrans] < this.concatenated[currentRef
										+ pointerCurrentRef]);
					} else if (concatenated[currentRef + pointerCurrentRef] < concatenated[trans + pointerTrans]) {
						// dismiss the suffix of ref which is not in trans
						do {
							this.concatenated[currentRef + pointerCurrentRef] = -1;
							pointerCurrentRef++;
						} while (pointerCurrentRef <= currentRefLength
								&& this.concatenated[currentRef + pointerCurrentRef] < concatenated[trans
										+ pointerTrans]);
					}
				}
			}
		}
	}

	private class ItemsIterator implements TransactionReader {
		private int nextItem;
		private int index;
		private int freq;
		private int max;

		public ItemsIterator(int tid) {
			int length = concatenated[tid];
			this.freq = concatenated[tid - 1];
			this.max = tid + length;
			this.index = tid + 1;
			this.findNext();
		}

		public boolean hasNext() {
			return this.nextItem >= 0;
		}

		public void findNext() {
			while (true) {
				if (this.index > this.max) {
					this.nextItem = -1;
					return;
				} else if (concatenated[index] >= 0) {
					this.nextItem = concatenated[index];
					this.index++;
					return;
				} else {
					this.index++;
				}
			}
		}

		public int next() {
			int res = this.nextItem;
			this.findNext();
			return res;
		}

		public int getTransactionSupport() {
			return this.freq;
		}

	}

	private class TransWriter implements TransactionsWriter {
		private int index = 2;
		private int tIdPosition = 1;
		private boolean sourceSorted;

		public TransWriter(boolean sourceSorted) {
			this.sourceSorted = sourceSorted;
		}

		public void addItem(int item) {
			concatenated[index] = item;
			this.index++;
		}

		public int endTransaction(int freq) {
			int size = index - tIdPosition - 1;
			concatenated[this.tIdPosition] = size;
			concatenated[this.tIdPosition - 1] = freq;
			int transId = this.tIdPosition;
			this.index++;
			this.tIdPosition = this.index;
			if (!this.sourceSorted) {
				Arrays.sort(concatenated, transId + 1, this.tIdPosition - 1);
			}
			this.index++;
			transactionsList.add(transId);
			return transId;
		}

		public TIntList getTids() {
			return null;
		}
	}

	public int computeTransactionsCount() {
		int i = 1;
		int nbTrans = 0;
		while (true) {
			if (this.concatenated[i] == 0) {
				break;
			} else {
				nbTrans++;
				i += this.concatenated[i] + 2;
			}
		}
		return nbTrans;
	}

	public int getErasedItemsCount() {
		int i = 1;
		int nbErased = 0;
		while (true) {
			if (this.concatenated[i] == 0) {
				break;
			} else {
				for (int j = 0; j < this.concatenated[i]; j++) {
					if (this.concatenated[j + i + 1] < 0) {
						nbErased++;
					}
				}
				i += this.concatenated[i] + 2;
			}
		}
		return nbErased;
	}
}
