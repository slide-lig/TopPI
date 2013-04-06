package fr.liglab.lcm.internals;

import java.util.Iterator;

import org.omg.CORBA.IntHolder;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.iterator.TIntIntIterator;
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
public class VIntConcatenatedDataset extends FilteredDataset {
	protected static int nbBytesForTransSize = 5;
	protected byte[] concatenated;
	private boolean sorted = false;

	public static void setMaxTransactionLength(int length) {
		int nbBits = ((int) Math.floor(Math.log(length) / (Math.log(2)))) + 1;
		nbBytesForTransSize = (int) Math.ceil(nbBits / 7.);
	}

	public VIntConcatenatedDataset(int minimumsupport, Iterator<int[]> transactions)
			throws DontExploreThisBranchException {
		super(minimumsupport, transactions);
	}

	public VIntConcatenatedDataset(IterableDataset parent, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {
		super(parent, extension, ignoreItems);
	}

	public VIntConcatenatedDataset(IterableDataset parent, int extension) throws DontExploreThisBranchException {
		super(parent, extension);
	}

	@Override
	protected void prepareTransactionsStructure(int sumOfRemainingItemsSupport, int distinctTransactionsLength,
			int distinctTransactionsCount) {
		int remainingItemsSize = 0;
		TIntIntIterator counts = this.supportCounts.iterator();
		while (counts.hasNext()) {
			counts.advance();
			remainingItemsSize += getVIntSize(counts.key()) * counts.value();
		}
		this.concatenated = new byte[remainingItemsSize + nbBytesForTransSize * this.transactionsCount];
	}

	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}

	@Override
	public Dataset createUnfilteredDataset(FilteredDataset upper, int extension) throws DontExploreThisBranchException {
		return new VIntConcatenatedUnfilteredDataset(upper, extension);
	}

	@Override
	public Dataset createFilteredDataset(FilteredDataset upper, int extension) throws DontExploreThisBranchException {
		return new VIntConcatenatedDataset(upper, extension);
	}

	static public int readVInt(byte[] array, IntHolder pointer) {
		byte b = array[pointer.value];
		pointer.value++;
		if (b >= 0) {
			return (int) b;
		} else {
			int res = (b & 0x7F);
			int shift = 7;
			while (true) {
				b = array[pointer.value];
				pointer.value++;
				if (b > 0) {
					res = res | (b << shift);
					break;
				} else {
					res = res | ((b & 0x7F) << shift);
					shift += 7;
				}
			}
			return res;
		}
	}

	static public void writeVInt(byte[] array, IntHolder pointer, int value) {
		while (true) {
			if (value >= 0 && value < 0x00000080) {
				array[pointer.value] = (byte) value;
				// System.out.println("encoding "
				// + String.format("%X", (byte) value));
				pointer.value++;
				break;
			} else {
				// System.out.println("encoding "
				// + String.format("%X", ((byte) value)) + " into "
				// + String.format("%X", (((byte) value) | 0x80)));
				array[pointer.value] = (byte) (((byte) value) | 0x80);
				value = value >>> 7;
				pointer.value++;
			}
		}
	}

	static public int getVIntSize(int val) {
		if (val < 0) {
			return 5;
		} else if (val < 0x00000080) {
			return 1;
		} else if (val < 0x00004000) {
			return 2;
		} else if (val < 0x00200000) {
			return 3;
		} else if (val < 0x10000000) {
			return 4;
		} else {
			return 5;
		}
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
		private int tid;
		private IntHolder index;

		public ItemsIterator(int tid) {
			this.tid = tid;
			this.index = new IntHolder(tid);
			int length = readVInt(concatenated, this.index);
			this.index.value = tid - length;
		}

		@Override
		public boolean hasNext() {
			return index.value < tid;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}

		@Override
		public int next() {
			return readVInt(concatenated, this.index);
		}

		@Override
		public int getTransactionSupport() {
			return 1;
		}
	}

	private class TransWriter implements TransactionsWriter {
		IntHolder index = new IntHolder(0);
		int transactionStart = 0;
		TIntList tids = null;

		public TransWriter() {
		}

		@Override
		public void addItem(int item) {
			writeVInt(concatenated, this.index, item);
		}

		@Override
		public int endTransaction(int freq) {
			int size = this.index.value - this.transactionStart;
			int transId = this.index.value;
			writeVInt(concatenated, this.index, size);
			int transLengthSize = this.index.value - transId;
			if (freq == 1) {
				this.transactionStart = this.index.value;
				return transId;
			} else {
				this.tids = new TIntArrayList(freq);
				this.tids.add(transId);
				int copySize = size + transLengthSize;
				for (int i = 1; i < freq; i++) {
					System.arraycopy(concatenated, this.transactionStart, concatenated, index.value, copySize);
					index.value += copySize;
					this.tids.add(index.value - transLengthSize);
				}
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

	@Override
	public int getRealSize() {
		return this.concatenated.length;
	}

	// public static void main(String[] args) throws Exception {
	// // int[] tests = { 127, 128, 2097152 - 1, 2097152, Integer.MAX_VALUE,
	// // Integer.MIN_VALUE };
	// // for (int t : tests) {
	// // setMaxTransactionLength(t);
	// // System.out.println(nbBytesForTransSize + " " + getVIntSize(t));
	// // System.out.println(String.format("%X", t));
	// // }
	// // IntHolder ih = new IntHolder(0);
	// // byte[] tab = new byte[5];
	// // int val = 127;
	// // System.out.println(String.format("%X", val));
	// // System.out.println(val);
	// // writeVInt(tab, ih, val);
	// // for (byte b : tab) {
	// // System.out.print(String.format("%X", b));
	// // }
	// // System.out.println();
	// // System.out.println(ih.value);
	// // ih.value = 0;
	// // System.out.println(readVInt(tab, ih));
	// // System.out.println(ih.value);
	// FilteredDataset d = new RebasedVIntConcatenatedDataset(5, new FileReader(
	// "/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// d = new VIntConcatenatedDataset(5, new
	// FileReader("/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// VIntConcatenatedDataset.setMaxTransactionLength(200);
	// d = new RebasedVIntConcatenatedDataset(5, new
	// FileReader("/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// d = new RebasedConcatenatedDataset(5, new
	// FileReader("/Users/vleroy/Workspace/lastfm/lastfm-s1200.dat"));
	// System.out.println(d.getRealSize());
	// d = null;
	// }
}
