package fr.liglab.lcm.internals;

import java.util.Iterator;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.iterator.TIntIterator;

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

	public ConcatenatedDataset(int minimumsupport, Iterator<int[]> transactions)
			throws DontExploreThisBranchException {
		super(minimumsupport, transactions);
	}

	public ConcatenatedDataset(IterableDataset parent, int extension,
			int[] ignoreItems) throws DontExploreThisBranchException {
		super(parent, extension, ignoreItems);
	}

	public ConcatenatedDataset(IterableDataset parent, int extension)
			throws DontExploreThisBranchException {
		super(parent, extension);
	}

	@Override
	protected void prepareTransactionsStructure(int remainingItemsCount) {
		this.concatenated = new int[remainingItemsCount
				+ this.transactionsCount];
	}

	@Override
	public int getRealSize() {
		return this.concatenated.length * Integer.SIZE;
	}

	@Override
	public Dataset getProjection(int extension)
			throws DontExploreThisBranchException {

		double extensionSupport = this.supportCounts.get(extension);

		if ((extensionSupport / this.transactionsCount) > UnfilteredDataset.FILTERING_THRESHOLD) {
			return new ConcatenatedUnfilteredDataset(this, extension);
		} else {
			return new ConcatenatedDataset(this, extension);
		}
	}

	@Override
	protected TIntIterator readTransaction(int tid) {
		return new ItemsIterator(tid);
	}

	@Override
	protected TransactionsWriter getTransactionsWriter() {
		return new TransWriter();
	}

	private class ItemsIterator implements TIntIterator {
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

	}

	private class TransWriter implements TransactionsWriter {
		int index = 1;
		int tIdPosition = 0;

		public TransWriter() {
		}

		@Override
		public void addItem(int item) {
			concatenated[index] = item;
			this.index++;
		}

		@Override
		public int endTransaction() {
			concatenated[this.tIdPosition] = index - tIdPosition - 1;
			int transId = this.tIdPosition;
			this.tIdPosition = this.index;
			this.index++;
			return transId;
		}

	}
}
