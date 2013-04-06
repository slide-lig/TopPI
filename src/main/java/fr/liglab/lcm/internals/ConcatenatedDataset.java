package fr.liglab.lcm.internals;

import java.util.Iterator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

/**
 * Here all transactions are prefixed by their length and concatenated in a
 * single int[]
 * 
 * This dataset internally performs - basic reduction : transactions contain
 * only items having a support count in [minusp, 100% [ - occurrence delivery :
 * occurrences are stored as indexes in the concatenated array - fast
 * prefix-preserving test (see inner class CandidatesIterator)
 */
public class ConcatenatedDataset extends FilteredDataset {

	private int[] concatenated;
	private boolean sorted = false;

	public ConcatenatedDataset(int minimumsupport, Iterator<int[]> transactions) throws DontExploreThisBranchException {
		super(minimumsupport, transactions);
	}

	public ConcatenatedDataset(IterableDataset parent, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(parent, extension, ignoreItems);
	}

	public ConcatenatedDataset(IterableDataset parent, int extension) throws DontExploreThisBranchException {
		super(parent, extension);
	}

	@Override
	protected void prepareTransactionsStructure(int sumOfRemainingItemsSupport, int distinctTransactionsLength,
			int distinctTransactionsCount) {
		this.concatenated = new int[sumOfRemainingItemsSupport + this.transactionsCount];
	}

	@Override
	public int getRealSize() {
		return this.concatenated.length * (Integer.SIZE / Byte.SIZE);
	}

	@Override
	public Dataset createUnfilteredDataset(FilteredDataset upper, int extension) throws DontExploreThisBranchException {
		return new ConcatenatedUnfilteredDataset(upper, extension);
	}

	@Override
	public Dataset createFilteredDataset(FilteredDataset upper, int extension) throws DontExploreThisBranchException {
		return new ConcatenatedDataset(upper, extension);
	}

	@Override
	protected TransactionReader readTransaction(int tid) {
		return new ItemsIterator(tid);
	}

	@Override
	protected TransactionsWriter getTransactionsWriter(boolean sourceSorted) {
		this.sorted = sourceSorted;
		return new TransWriter();
	}

	@Override
	public boolean itemsSorted() {
		return this.sorted;
	}

	private class ItemsIterator implements TransactionReader {
		private int max;
		private int index;

		public ItemsIterator(int tid) {
			int length = concatenated[tid];
			this.max = tid + length;
			this.index = tid + 1;
		}

		@Override
		public boolean hasNext() {
			return this.index <= this.max;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}

		@Override
		public int next() {
			int val = concatenated[this.index];
			this.index++;
			return val;
		}

		@Override
		public int getTransactionSupport() {
			return 1;
		}

	}

	private class TransWriter implements TransactionsWriter {
		int index = 1;
		int tIdPosition = 0;
		TIntList tids = null;

		public TransWriter() {
		}

		@Override
		public void addItem(int item) {
			concatenated[index] = item;
			this.index++;
		}

		@Override
		public int endTransaction(int freq) {
			int size = index - tIdPosition - 1;
			concatenated[this.tIdPosition] = size;
			int transId = this.tIdPosition;
			if (freq == 1) {
				this.tIdPosition = this.index;
				this.index++;
				return transId;
			} else {
				this.tids = new TIntArrayList(freq);
				this.tids.add(transId);
				for (int i = 1; i < freq; i++) {
					this.tids.add(index);
					System.arraycopy(concatenated, this.tIdPosition, concatenated, index, size + 1);
					index += (size + 1);
				}
				this.tIdPosition = this.index;
				this.index++;
				return -1;
			}
		}

		@Override
		public TIntList getTids() {
			TIntList localTids = this.tids;
			this.tids = null;
			return localTids;
		}
	}
}
