package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.Arrays;
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
public class FPTreeMixDataset extends FilteredDataset {

	protected int[] concatenated;
	protected TIntList fptree;
	static protected int fptreeLimit = 10;

	public FPTreeMixDataset(int minimumsupport, Iterator<int[]> transactions) throws DontExploreThisBranchException {
		super(minimumsupport, transactions);
	}

	public FPTreeMixDataset(IterableDataset parent, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(parent, extension, ignoreItems);
	}

	public FPTreeMixDataset(IterableDataset parent, int extension) throws DontExploreThisBranchException {
		super(parent, extension);
	}

	@Override
	protected void prepareTransactionsStructure(int sumOfRemainingItemsSupport, int distinctTransactionsLength,
			int distinctTransactionsCount) {
		int sumAboveLimitSupport = 0;
		TIntIntIterator counts = this.supportCounts.iterator();
		while (counts.hasNext()) {
			counts.advance();
			if (counts.key() > fptreeLimit) {
				sumAboveLimitSupport += counts.value();
			}
		}
		this.concatenated = new int[sumAboveLimitSupport + 2 * this.transactionsCount];
		// init with minimal size, cannot predict exact size
		this.fptree = new TIntArrayList(fptreeLimit * 2);
	}

	@Override
	public int getRealSize() {
		return (this.concatenated.length + this.fptree.size()) * (Integer.SIZE / Byte.SIZE);
	}

	@Override
	public Dataset createUnfilteredDataset(FilteredDataset upper, int extension) throws DontExploreThisBranchException {
		return new ConcatenatedUnfilteredDataset(upper, extension);
	}

	@Override
	public Dataset createFilteredDataset(FilteredDataset upper, int extension) throws DontExploreThisBranchException {
		return new FPTreeMixDataset(upper, extension);
	}

	@Override
	protected TransactionReader readTransaction(int tid) {
		return new ItemsIterator(tid);
	}

	@Override
	protected TransactionsWriter getTransactionsWriter(boolean sourceSorted) {
		if (sourceSorted) {
			return new TransWriterSorted();
		} else {
			return new TransWriterUnsorted();
		}
	}

	@Override
	public boolean itemsSorted() {
		return true;
	}

	private class ItemsIterator implements TransactionReader {
		private int index;
		private boolean inFPTree;
		private int tid;
		private int length;

		public ItemsIterator(int tid) {
			this.length = concatenated[tid];
			this.tid = tid;
			int fptreeStart = concatenated[tid + 1];
			if (fptreeStart == -1) {
				this.inFPTree = false;
				this.index = this.tid + 2;
			} else {
				this.inFPTree = true;
				this.index = fptreeStart;
			}
		}
		
		public boolean hasNext() {
			if (inFPTree) {
				return true;
			} else {
				return this.length > 0;
			}
		}
		
		public int next() {
			int val;
			if (this.inFPTree) {
				val = fptree.get(this.index);
				this.index = fptree.get(this.index + 1);
				if (this.index == -1) {
					this.inFPTree = false;
					this.index = this.tid + 2;
				}
			} else {
				val = concatenated[this.index];
				this.length--;
				this.index++;
			}
			// System.out.println("reading " + val);
			return val;
		}
		
		public int getTransactionSupport() {
			return 1;
		}

	}

	private class TransWriterSorted implements TransactionsWriter {
		private final TObjectIntMap<WrappedArray> fpTreeIndex;
		protected int index = 1;
		protected int tIdPosition = 0;
		protected final TIntList buffer;
		protected TIntList tids = null;
		private boolean fpTreeDone = false;

		public TransWriterSorted() {
			this.buffer = new TIntArrayList(fptreeLimit);
			this.fpTreeIndex = new TObjectIntHashMap<WrappedArray>();
		}
		
		public void addItem(int item) {
			if (item > fptreeLimit) {
				if (!fpTreeDone) {
					if (!this.buffer.isEmpty()) {
						concatenated[this.index] = this.getFpTreePos(this.buffer.toArray());
						this.buffer.clear();
					} else {
						concatenated[this.index] = -1;
					}
					this.index++;
					this.fpTreeDone = true;
				}
				concatenated[this.index] = item;
				this.index++;
			} else {
				this.buffer.add(item);
			}
		}
		
		public int endTransaction(int freq) {
			if (!fpTreeDone) {
				if (!this.buffer.isEmpty()) {
					concatenated[this.index] = this.getFpTreePos(this.buffer.toArray());
					this.buffer.clear();
				} else {
					concatenated[this.index] = -1;
				}
				this.index++;
				this.fpTreeDone = true;
			}
			int size = index - tIdPosition - 2;
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
					System.arraycopy(concatenated, this.tIdPosition, concatenated, index, size + 2);
					index += (size + 2);
				}
				this.tIdPosition = this.index;
				this.index++;
				return -1;
			}
		}

		protected int getFpTreePos(int[] a) {
			WrappedArray wa = new WrappedArray(a);
			do {
				if (this.fpTreeIndex.containsKey(wa)) {
					int pos = this.fpTreeIndex.get(wa);
					// System.out.println("got a match with " + wa + " at " +
					// pos);
					for (int i = wa.getSize(); i < a.length; i++) {
						int v = a[i];
						fptree.add(v);
						fptree.add(pos);
						pos = fptree.size() - 2;
					}
					if (a.length != wa.getSize()) {
						this.fpTreeIndex.put(new WrappedArray(a), pos);
						// System.out.println("partial insert " +
						// Arrays.toString(a) + " at " + pos);
					}
					return pos;
				}
			} while (wa.decSize() > 0);
			// insert the full array
			int pos = -1;
			for (int v : a) {
				fptree.add(v);
				fptree.add(pos);
				pos = fptree.size() - 2;
			}
			this.fpTreeIndex.put(new WrappedArray(a), pos);
			// System.out.println("full insert " + Arrays.toString(a) + " at " +
			// pos);
			return pos;
		}
		
		public TIntList getTids() {
			TIntList localTids = this.tids;
			this.tids = null;
			return localTids;
		}
	}

	private class TransWriterUnsorted extends TransWriterSorted {

		public TransWriterUnsorted() {
			super();
			this.index = 0;
		}

		@Override
		public void addItem(int item) {
			// System.out.println("writing " + item);
			this.buffer.add(item);
		}

		@Override
		public int endTransaction(int freq) {
			this.buffer.sort();
			int limit = this.buffer.binarySearch(fptreeLimit);
			if (limit >= 0) {
				limit++;
			} else {
				limit = (-limit) - 1;
			}
			this.index += 2;
			for (int i = limit; i < this.buffer.size(); i++) {
				concatenated[this.index] = this.buffer.get(i);
				this.index++;
			}
			if (limit > 0) {
				concatenated[this.tIdPosition + 1] = this.getFpTreePos(this.buffer.toArray(0, limit));
			} else {
				concatenated[this.tIdPosition + 1] = -1;
			}
			this.buffer.clear();
			int size = this.index - this.tIdPosition - 2;
			concatenated[this.tIdPosition] = size;
			int transId = this.tIdPosition;
			if (freq == 1) {
				this.tIdPosition = this.index;
				return transId;
			} else {
				this.tids = new TIntArrayList(freq);
				this.tids.add(transId);
				for (int i = 1; i < freq; i++) {
					this.tids.add(index);
					System.arraycopy(concatenated, this.tIdPosition, concatenated, index, size + 2);
					index += (size + 2);
				}
				this.tIdPosition = this.index;
				return -1;
			}
		}
	}

	private class WrappedArray {
		private int[] array;
		private int size;

		public WrappedArray(int[] array) {
			super();
			this.array = array;
			this.size = array.length;
		}

		@Override
		public int hashCode() {
			int hash = 1;
			for (int i = 0; i < this.size; i++) {
				hash = 31 * hash + this.array[i];
			}
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			WrappedArray other = (WrappedArray) obj;
			if (this.size != other.size)
				return false;
			for (int i = 0; i < size; i++) {
				if (this.array[i] != other.array[i]) {
					return false;
				}
			}
			return true;
		}

		public int decSize() {
			this.size--;
			return this.size;
		}

		public final int getSize() {
			return this.size;
		}

		@Override
		public String toString() {
			return "WrappedArray [array=" + Arrays.toString(array) + ", size=" + size + "]";
		}

	}
}
